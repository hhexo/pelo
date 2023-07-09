use chrono::{DateTime, Days, Utc};
use rand::distributions::{Distribution, Uniform};
use rand::prelude::*;

use crate::data::{Rating, Task, Vote};
use crate::elo::{new_elo_pair, Outcome};
use crate::errors::{Error, ErrorCode};
use crate::persistence::Persistence;

use std::time::SystemTime;

const MAX_OPTIMISTIC_CONCURRENCY_ATTEMPTS: i32 = 8;

pub struct Engine;

impl Engine {
    pub fn get_question(&self, persistence: &impl Persistence) -> Result<(Task, Task), Error> {
        let tasks: Vec<Task> = persistence
            .list_tasks()?
            .iter()
            .filter(|t| !t.closed())
            .cloned()
            .collect();
        if tasks.len() < 2 {
            return Err(Error::not_enough_tasks());
        }
        let distribution = Uniform::from(0..tasks.len());
        let mut rng = thread_rng();
        let t0 = distribution.sample(&mut rng);
        let mut t1 = distribution.sample(&mut rng);
        while t1 == t0 {
            t1 = distribution.sample(&mut rng);
        }
        Ok((tasks[t0].clone(), tasks[t1].clone()))
    }

    pub fn answer_question(
        &self,
        persistence: &mut impl Persistence,
        u_id: &str,
        t0: &Task,
        t1: &Task,
        outcome: Outcome,
    ) -> Result<(), Error> {
        let user = persistence.get_user(u_id)?;
        if user.is_limited() {
            let now: DateTime<Utc> = SystemTime::now().into();
            let last_week = now
                .checked_sub_days(Days::new(7))
                .ok_or(Error::generic("date wrap-around"))?;
            let user_votes = persistence.get_num_votes_for_user_since(u_id, &last_week)? as i32;
            if user_votes >= user.limit_votes_per_week() {
                return Err(Error::user_limit_exceeded(u_id));
            }
        }
        // Optimistic concurrency based on OffsetToken
        let mut attempts = 0;
        'out: loop {
            let snapshot = persistence.get_snapshot()?;
            let mut r0 = snapshot
                .ranking()
                .iter()
                .find(|rating| rating.task() == t0.id())
                .unwrap_or(&Rating::new(t0.id().clone()))
                .clone();
            let mut r1 = snapshot
                .ranking()
                .iter()
                .find(|rating| rating.task() == t1.id())
                .unwrap_or(&Rating::new(t1.id().clone()))
                .clone();
            let (new_elo0, new_elo1) = new_elo_pair(r0.elo(), r1.elo(), outcome);
            let vote = Vote::new(
                u_id,
                SystemTime::now().into(),
                t0.id().clone(),
                t1.id().clone(),
                outcome,
            );
            r0 = Rating::with_elo(t0.id().clone(), new_elo0);
            r1 = Rating::with_elo(t1.id().clone(), new_elo1);
            match persistence.add_vote_and_update_ratings(snapshot.etag(), &vote, &r0, &r1) {
                Ok(_) => {
                    break 'out;
                }
                Err(e) => {
                    if e.code() != ErrorCode::OptimisticConcurrencyRetryTransaction {
                        return Err(e);
                    }
                    // check the max attempts
                    if attempts >= MAX_OPTIMISTIC_CONCURRENCY_ATTEMPTS {
                        return Err(Error::too_many_retry_attempts());
                    }
                    // otherwise retry
                    attempts += 1;
                }
            }
        }
        Ok(())
    }

    pub fn get_current_ranking(
        &self,
        persistence: &mut impl Persistence,
    ) -> Result<Vec<Rating>, Error> {
        let snapshot = persistence.get_snapshot()?;
        Ok(snapshot.ranking().clone())
    }
}

#[cfg(test)]
mod tests {
    use crate::data::{Task, User};
    use crate::elo::Outcome;
    use crate::engine::Engine;
    use crate::errors::ErrorCode;
    use crate::persistence::{InMemory, Persistence};

    use url::Url;
    use uuid::Uuid;

    const EPSILON: f32 = 0.000001;

    const TEST_USER_ID: &'static str = "test_user";
    const TEST_USER_LIMIT: i32 = 2;

    const TEST_TASK_SUMMARY_0: &'static str = "task zero";
    const TEST_TASK_SUMMARY_1: &'static str = "task one";

    fn init(database: &mut impl Persistence) {
        database
            .upsert_user(&User::new(TEST_USER_ID, TEST_USER_LIMIT))
            .unwrap();
        database
            .upsert_task(&Task::new(
                Uuid::new_v4(),
                TEST_TASK_SUMMARY_0,
                Url::parse("https://localhost/0").unwrap(),
                false,
            ))
            .unwrap();
        database
            .upsert_task(&Task::new(
                Uuid::new_v4(),
                TEST_TASK_SUMMARY_1,
                Url::parse("https://localhost/1").unwrap(),
                false,
            ))
            .unwrap();
    }

    #[test]
    fn test_question() {
        let mut database = InMemory::new();
        let engine = Engine;

        let result0 = engine.get_question(&database);
        assert!(result0.is_err());
        assert_eq!(result0.err().unwrap().code(), ErrorCode::NotEnoughTasks);

        init(&mut database);

        let result1 = engine.get_question(&database);
        assert!(result1.is_ok());
        let (t0, t1) = result1.ok().unwrap();
        assert!(t0.summary() == TEST_TASK_SUMMARY_0 || t1.summary() == TEST_TASK_SUMMARY_0);
        assert!(t0.summary() == TEST_TASK_SUMMARY_1 || t1.summary() == TEST_TASK_SUMMARY_1);
        assert_ne!(t0.summary(), t1.summary());
    }

    #[test]
    fn test_answer_no_user() {
        let mut database = InMemory::new();
        init(&mut database);
        let engine = Engine;
        let (t0, t1) = engine.get_question(&database).unwrap();

        let result0 = engine.answer_question(&mut database, "not_a_user", &t0, &t1, Outcome::Draw);
        assert!(result0.is_err());
        assert_eq!(result0.err().unwrap().code(), ErrorCode::UserNotFound);
    }

    #[test]
    fn test_answer_user_limit_exceeded() {
        let mut database = InMemory::new();
        init(&mut database);
        let engine = Engine;
        let (t0, t1) = engine.get_question(&database).unwrap();

        engine
            .answer_question(&mut database, TEST_USER_ID, &t0, &t1, Outcome::Draw)
            .unwrap();
        engine
            .answer_question(&mut database, TEST_USER_ID, &t0, &t1, Outcome::Draw)
            .unwrap();
        let result0 = engine.answer_question(&mut database, TEST_USER_ID, &t0, &t1, Outcome::Draw);
        assert!(result0.is_err());
        assert_eq!(result0.err().unwrap().code(), ErrorCode::UserLimitExceeded);
    }

    #[test]
    fn test_answer_task_not_found() {
        let mut database = InMemory::new();
        init(&mut database);
        let engine = Engine;
        let (t0, t1) = engine.get_question(&database).unwrap();
        let t2 = Task::new(Uuid::new_v4(), t0.summary(), t0.link().clone(), false);

        let result0 = engine.answer_question(&mut database, TEST_USER_ID, &t2, &t1, Outcome::Draw);
        assert!(result0.is_err());
        assert_eq!(result0.err().unwrap().code(), ErrorCode::TaskNotFound);
    }

    #[test]
    fn test_answer_success() {
        let mut database = InMemory::new();
        init(&mut database);
        let engine = Engine;

        let mut ranking = engine.get_current_ranking(&mut database).unwrap();
        assert_eq!(ranking.len(), 2);
        assert_ne!(ranking[0].task(), ranking[1].task());
        assert!((ranking[0].elo() - 1200.0).abs() < EPSILON);
        assert!((ranking[1].elo() - 1200.0).abs() < EPSILON);

        let (t0, t1) = engine.get_question(&database).unwrap();
        let result0 = engine.answer_question(&mut database, TEST_USER_ID, &t0, &t1, Outcome::P0Win);
        assert!(result0.is_ok());

        ranking = engine.get_current_ranking(&mut database).unwrap();
        assert_eq!(ranking.len(), 2);
        assert_ne!(ranking[0].task(), ranking[1].task());
        if t0.id() == ranking[0].task() {
            assert!((ranking[0].elo() - 1216.0).abs() < EPSILON);
            assert!((ranking[1].elo() - 1184.0).abs() < EPSILON);
        } else {
            assert!((ranking[0].elo() - 1184.0).abs() < EPSILON);
            assert!((ranking[1].elo() - 1216.0).abs() < EPSILON);
        }
    }
}
