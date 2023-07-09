use chrono::{DateTime, Utc};
use url::Url;
use uuid::Uuid;

use crate::data::{Rating, Task, User, Vote};
use crate::elo::Outcome;
use crate::errors::Error;

#[derive(Debug, Clone)]
pub struct Etag {
    pub token: String,
}

#[derive(Debug, Clone)]
pub struct Snapshot {
    etag: Etag,
    ranking: Vec<Rating>,
}
impl Snapshot {
    pub fn etag(&self) -> &Etag {
        &self.etag
    }
    pub fn ranking(&self) -> &Vec<Rating> {
        &self.ranking
    }
}

pub trait Persistence {
    fn list_users(&self) -> Result<Vec<User>, Error>;

    fn upsert_user(&mut self, u: &User) -> Result<(), Error>;

    fn get_user(&self, u_id: &str) -> Result<User, Error>;

    fn get_num_votes_for_user_since(
        &self,
        u_id: &str,
        since: &DateTime<Utc>,
    ) -> Result<usize, Error>;

    fn list_tasks(&self) -> Result<Vec<Task>, Error>;

    fn upsert_task(&mut self, t: &Task) -> Result<(), Error>;

    fn close_task(&mut self, t_id: &Uuid) -> Result<(), Error>;

    fn get_snapshot(&mut self) -> Result<Snapshot, Error>;

    fn add_vote_and_update_ratings(
        &mut self,
        etag: &Etag,
        vote: &Vote,
        r0: &Rating,
        r1: &Rating,
    ) -> Result<(), Error>;
}

// --- Implementations --------------------------------------------------------

use std::collections::HashMap;

struct InMemoryInner {
    users: HashMap<String, User>,
    tasks: HashMap<Uuid, Task>,
    current_ranking: HashMap<Uuid, f32>,
    votes: Vec<Vote>,
}
impl InMemoryInner {
    fn new() -> Self {
        InMemoryInner {
            users: HashMap::new(),
            tasks: HashMap::new(),
            current_ranking: HashMap::new(),
            votes: Vec::new(),
        }
    }
}
impl Persistence for InMemoryInner {
    fn list_users(&self) -> Result<Vec<User>, Error> {
        Ok(self.users.iter().map(|(_, v)| v).cloned().collect())
    }

    fn upsert_user(&mut self, u: &User) -> Result<(), Error> {
        self.users.insert(u.id().to_string(), u.clone());
        Ok(())
    }

    fn get_user(&self, u_id: &str) -> Result<User, Error> {
        Ok(self
            .users
            .get(u_id)
            .ok_or(Error::user_not_found(u_id))?
            .clone())
    }

    fn get_num_votes_for_user_since(
        &self,
        u_id: &str,
        since: &DateTime<Utc>,
    ) -> Result<usize, Error> {
        Ok(self
            .votes
            .iter()
            .filter(|v| v.time() >= since)
            .filter(|v| v.voter() == u_id)
            .count())
    }

    fn list_tasks(&self) -> Result<Vec<Task>, Error> {
        Ok(self.tasks.iter().map(|(_, v)| v).cloned().collect())
    }

    fn upsert_task(&mut self, t: &Task) -> Result<(), Error> {
        self.tasks.insert(t.id().clone(), t.clone());
        self.current_ranking
            .insert(t.id().clone(), Rating::new(t.id().clone()).elo());
        Ok(())
    }

    fn close_task(&mut self, t_id: &Uuid) -> Result<(), Error> {
        self.tasks
            .get_mut(t_id)
            .ok_or(Error::task_not_found(t_id))?
            .close();
        Ok(())
    }

    fn get_snapshot(&mut self) -> Result<Snapshot, Error> {
        let mut has_nans = false;
        let mut ranking: Vec<Rating> = self
            .current_ranking
            .iter()
            .map(|(k, v)| {
                if v.is_nan() {
                    has_nans = true;
                }
                Rating::with_elo(k.clone(), v.clone())
            })
            .collect();
        if !has_nans {
            ranking.sort_by(|a, b| a.elo().partial_cmp(&b.elo()).unwrap());
        }
        Ok(Snapshot {
            ranking: ranking,
            etag: Etag {
                token: format!("{}", self.votes.len()),
            },
        })
    }

    fn add_vote_and_update_ratings(
        &mut self,
        etag: &Etag,
        vote: &Vote,
        r0: &Rating,
        r1: &Rating,
    ) -> Result<(), Error> {
        let etag_usize: usize = etag
            .token
            .parse()
            .map_err(|_| Error::generic("etag parse error"))?;
        if etag_usize != self.votes.len() {
            return Err(Error::retry_transaction());
        }
        if !self.tasks.contains_key(r0.task()) {
            return Err(Error::task_not_found(r0.task()));
        }
        if !self.tasks.contains_key(r1.task()) {
            return Err(Error::task_not_found(r1.task()));
        }
        self.current_ranking.insert(r0.task().clone(), r0.elo());
        self.current_ranking.insert(r1.task().clone(), r1.elo());
        self.votes.push(vote.clone());
        Ok(())
    }
}

pub struct InMemory {
    data: std::sync::Mutex<InMemoryInner>,
}
impl InMemory {
    pub fn new() -> Self {
        InMemory {
            data: std::sync::Mutex::new(InMemoryInner::new()),
        }
    }
}
impl Persistence for InMemory {
    fn list_users(&self) -> Result<Vec<User>, Error> {
        self.data.lock().unwrap().list_users()
    }

    fn upsert_user(&mut self, u: &User) -> Result<(), Error> {
        self.data.lock().unwrap().upsert_user(u)
    }

    fn get_user(&self, u_id: &str) -> Result<User, Error> {
        self.data.lock().unwrap().get_user(u_id)
    }

    fn get_num_votes_for_user_since(
        &self,
        u_id: &str,
        since: &DateTime<Utc>,
    ) -> Result<usize, Error> {
        self.data
            .lock()
            .unwrap()
            .get_num_votes_for_user_since(u_id, since)
    }

    fn list_tasks(&self) -> Result<Vec<Task>, Error> {
        self.data.lock().unwrap().list_tasks()
    }

    fn upsert_task(&mut self, t: &Task) -> Result<(), Error> {
        self.data.lock().unwrap().upsert_task(t)
    }

    fn close_task(&mut self, t_id: &Uuid) -> Result<(), Error> {
        self.data.lock().unwrap().close_task(t_id)
    }

    fn get_snapshot(&mut self) -> Result<Snapshot, Error> {
        self.data.lock().unwrap().get_snapshot()
    }

    fn add_vote_and_update_ratings(
        &mut self,
        etag: &Etag,
        vote: &Vote,
        r0: &Rating,
        r1: &Rating,
    ) -> Result<(), Error> {
        self.data
            .lock()
            .unwrap()
            .add_vote_and_update_ratings(etag, vote, r0, r1)
    }
}

use rusqlite;

pub struct SQLitePersistence {
    connection: rusqlite::Connection,
}
impl SQLitePersistence {
    pub fn new(db_path: std::path::PathBuf) -> Result<Self, Error> {
        let mut conn = rusqlite::Connection::open(&db_path)?;
        conn.execute(
            "create table if not exists pelo_global_etag (
                 id integer primary key,
                 token text not null
             )",
            (),
        )?;
        conn.execute(
            "create table if not exists pelo_users (
                 id text primary key,
                 limit_votes_per_week integer not null
             )",
            (),
        )?;
        conn.execute(
            "create table if not exists pelo_tasks (
                 id text primary key,
                 summary text not null,
                 link text,
                 closed integer
             )",
            (),
        )?;
        conn.execute(
            "create table if not exists pelo_ratings (
                 task text not null,
                 elo real not null
             )",
            (),
        )?;
        conn.execute(
            "create table if not exists pelo_votes (
                 voter text not null,
                 time text not null,
                 task0 text not null,
                 task1 text not null,
                 outcome integer
             )",
            (),
        )?;
        conn.execute(
            "create index if not exists pelo_votes_by_user_and_time 
                 on pelo_votes(voter, time)",
            (),
        )?;

        let tx = conn.transaction()?;
        tx.execute(
            "insert into pelo_global_etag (id, token)
                    values (0, ?1)
                    on conflict(id) do update set token = ?1",
            (Uuid::new_v4().to_string(),),
        )?;
        tx.commit()?;

        Ok(SQLitePersistence { connection: conn })
    }
}

impl Persistence for SQLitePersistence {
    fn list_users(&self) -> Result<Vec<User>, Error> {
        let mut stmt = self
            .connection
            .prepare("SELECT id, limit_votes_per_week FROM pelo_users")?;
        let mut result = Vec::new();
        stmt.query_map([], |row| {
            let id: String = row.get(0)?;
            let limit: i32 = row.get(1)?;
            Ok(User::new(&id, limit))
        })?
        .try_for_each(|maybe_user| -> Result<(), Error> {
            result.push(maybe_user?);
            Ok(())
        })?;
        Ok(result)
    }

    fn upsert_user(&mut self, u: &User) -> Result<(), Error> {
        self.connection.execute(
            "insert into pelo_users(id, limit_votes_per_week)
             values (?1, ?2)
             on conflict(id) do update set limit_votes_per_week = ?2",
            (u.id(), u.limit_votes_per_week()),
        )?;
        Ok(())
    }

    fn get_user(&self, u_id: &str) -> Result<User, Error> {
        let mut stmt = self.connection.prepare(
            "SELECT id, limit_votes_per_week FROM pelo_users
             WHERE id = ?1",
        )?;
        let mut result = Vec::new();
        stmt.query_map(rusqlite::params![u_id], |row| {
            let id: String = row.get(0)?;
            let limit: i32 = row.get(1)?;
            Ok(User::new(&id, limit))
        })?
        .try_for_each(|maybe_user| -> Result<(), Error> {
            result.push(maybe_user?);
            Ok(())
        })?;
        if result.len() != 1 {
            return Err(Error::db_error("more than one user with same primary key"));
        }
        Ok(result[0].clone())
    }

    fn get_num_votes_for_user_since(
        &self,
        u_id: &str,
        since: &DateTime<Utc>,
    ) -> Result<usize, Error> {
        let result: usize = self.connection.query_row(
            "SELECT COUNT(*) FROM pelo_votes where voter = ?1 AND time > ?2",
            [u_id, &since.to_rfc3339()],
            |row| row.get(0),
        )?;
        Ok(result)
    }

    fn list_tasks(&self) -> Result<Vec<Task>, Error> {
        let mut stmt = self
            .connection
            .prepare("SELECT id, summary, link, closed FROM pelo_tasks")?;
        let mut result = Vec::new();
        stmt.query_map([], |row| {
            let id_: String = row.get(0)?;
            let id: Uuid = Uuid::parse_str(&id_).unwrap();
            let summary: String = row.get(1)?;
            let link_: String = row.get(2)?;
            let link: Url = Url::parse(&link_).unwrap();
            let closed: i32 = row.get(3)?;
            Ok(Task::new(id, &summary, link, closed != 0))
        })?
        .try_for_each(|maybe_task| -> Result<(), Error> {
            result.push(maybe_task?);
            Ok(())
        })?;
        Ok(result)
    }

    fn upsert_task(&mut self, t: &Task) -> Result<(), Error> {
        let rating = Rating::new(t.id().clone());
        let transaction = self.connection.transaction()?;

        transaction.execute(
            "insert into pelo_tasks(id, summary, link, closed) 
             values (?1, ?2, ?3, ?4)
             on conflict(id) do update set (summary, link, closed) = (?2, ?3, ?4)",
            (
                &t.id().to_string(),
                t.summary(),
                &t.link().to_string(),
                if t.closed() { 1 } else { 0 },
            ),
        )?;
        transaction.execute(
            "insert into pelo_ratings(task, elo)
             values (?1, ?2)",
            (&rating.task().to_string(), rating.elo()),
        )?;

        transaction.commit()?;
        Ok(())
    }

    fn close_task(&mut self, t_id: &Uuid) -> Result<(), Error> {
        self.connection.execute(
            "update pelo_tasks set closed = 1 where id = ?1",
            (&t_id.to_string(),),
        )?;
        Ok(())
    }

    fn get_snapshot(&mut self) -> Result<Snapshot, Error> {
        let token: String =
            self.connection
                .query_row("SELECT token FROM pelo_global_etag", [], |row| row.get(0))?;

        let mut stmt = self
            .connection
            .prepare("SELECT task, elo FROM pelo_ratings")?;
        let mut ranking = Vec::new();
        stmt.query_map([], |row| {
            let id_: String = row.get(0)?;
            let id: Uuid = Uuid::parse_str(&id_).unwrap();
            let elo: f32 = row.get(1)?;
            Ok(Rating::with_elo(id, elo))
        })?
        .try_for_each(|maybe_rating| -> Result<(), Error> {
            ranking.push(maybe_rating?);
            Ok(())
        })?;
        Ok(Snapshot {
            ranking: ranking,
            etag: Etag { token: token },
        })
    }

    fn add_vote_and_update_ratings(
        &mut self,
        etag: &Etag,
        vote: &Vote,
        r0: &Rating,
        r1: &Rating,
    ) -> Result<(), Error> {
        let transaction = self.connection.transaction()?;
        let token: String =
            transaction.query_row("SELECT token FROM pelo_global_etag", [], |row| row.get(0))?;

        if token != etag.token {
            return Err(Error::retry_transaction());
        }

        transaction.execute(
            "update pelo_ratings set elo = ?2 where task = ?1",
            (&r0.task().to_string(), &r0.elo()),
        )?;
        transaction.execute(
            "update pelo_ratings set elo = ?2 where task = ?1",
            (&r1.task().to_string(), &r1.elo()),
        )?;
        transaction.execute(
            "insert into pelo_votes values(?1, ?2, ?3, ?4, ?5)",
            (
                vote.voter(),
                &vote.time().to_rfc3339(),
                &vote.task0().to_string(),
                &vote.task1().to_string(),
                match vote.outcome() {
                    Outcome::P0Win => -1,
                    Outcome::Draw => 0,
                    Outcome::P1Win => 1,
                },
            ),
        )?;
        transaction.execute(
            "insert into pelo_global_etag (id, token)
                    values (0, ?1)
                    on conflict(id) do update set token = ?1",
            (Uuid::new_v4().to_string(),),
        )?;

        transaction.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::data::{Rating, Task, User, Vote};
    use crate::elo::Outcome;
    use crate::errors::ErrorCode;
    use crate::persistence::{Persistence, SQLitePersistence};

    use url::Url;
    use uuid::Uuid;

    const EPSILON: f32 = 0.000001;

    const TEST_USER_ID: &'static str = "test_user";
    const TEST_USER_LIMIT: i32 = 2;

    const TEST_TASK_SUMMARY_0: &'static str = "task zero";
    const TEST_TASK_SUMMARY_1: &'static str = "task one";

    const TEST_SQLITE_PATH: &'static str = "/tmp/pelo-test-sqlite.db";

    static TEST_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn init_sqlite() -> SQLitePersistence {
        // Delete a file if it was present
        let _ = std::fs::remove_file(TEST_SQLITE_PATH);

        let mut database = SQLitePersistence::new(TEST_SQLITE_PATH.into()).unwrap();
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

        database
    }

    fn destroy_sqlite(s: &mut SQLitePersistence) {
        s.connection.execute("drop table pelo_users", ()).unwrap();
        s.connection.execute("drop table pelo_tasks", ()).unwrap();
        s.connection.execute("drop table pelo_ratings", ()).unwrap();
        s.connection.execute("drop table pelo_votes", ()).unwrap();
        s.connection
            .execute("drop table pelo_global_etag", ())
            .unwrap();
    }

    #[test]
    fn test_sqlite_creation() {
        let _guard = TEST_MUTEX.lock().unwrap();
        let mut database = init_sqlite();
        destroy_sqlite(&mut database);
    }

    #[test]
    fn test_sqlite_read_users() {
        let _guard = TEST_MUTEX.lock().unwrap();
        let mut database = init_sqlite();

        let result0 = database.list_users();
        assert!(result0.is_ok());
        let users = result0.unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].id(), TEST_USER_ID);
        assert_eq!(users[0].limit_votes_per_week(), TEST_USER_LIMIT);

        let result1 = database.get_user(TEST_USER_ID);
        let user = result1.unwrap();
        assert_eq!(user.id(), TEST_USER_ID);
        assert_eq!(user.limit_votes_per_week(), TEST_USER_LIMIT);

        destroy_sqlite(&mut database);
    }

    #[test]
    fn test_sqlite_handle_tasks() {
        let _guard = TEST_MUTEX.lock().unwrap();
        let mut database = init_sqlite();

        let result0 = database.list_tasks();
        assert!(result0.is_ok());
        let mut tasks = result0.unwrap();
        assert_eq!(tasks.len(), 2);
        assert!(
            tasks[0].summary() == TEST_TASK_SUMMARY_0 || tasks[0].summary() == TEST_TASK_SUMMARY_1
        );
        assert!(
            tasks[1].summary() == TEST_TASK_SUMMARY_0 || tasks[1].summary() == TEST_TASK_SUMMARY_1
        );
        assert_ne!(tasks[0].summary(), tasks[1].summary());

        let changed_task_id = tasks[0].id().clone();
        let result1 = database.close_task(&changed_task_id);
        assert!(result1.is_ok());

        let result2 = database.list_tasks();
        assert!(result2.is_ok());
        tasks = result2.unwrap();
        assert_eq!(tasks.len(), 2);
        assert!(
            tasks[0].summary() == TEST_TASK_SUMMARY_0 || tasks[0].summary() == TEST_TASK_SUMMARY_1
        );
        assert!(
            tasks[1].summary() == TEST_TASK_SUMMARY_0 || tasks[1].summary() == TEST_TASK_SUMMARY_1
        );
        assert_ne!(tasks[0].summary(), tasks[1].summary());
        if tasks[0].id() == &changed_task_id {
            assert!(tasks[0].closed());
        }
        if tasks[1].id() == &changed_task_id {
            assert!(tasks[1].closed());
        }

        destroy_sqlite(&mut database);
    }

    use chrono::{DateTime, Days, Utc};

    #[test]
    fn test_sqlite_handle_votes() {
        let _guard = TEST_MUTEX.lock().unwrap();
        let mut database = init_sqlite();

        let result0 = database.list_tasks();
        assert!(result0.is_ok());
        let tasks = result0.unwrap();
        assert_eq!(tasks.len(), 2);
        let t0 = tasks[0].clone();
        let t1 = tasks[1].clone();

        let now: DateTime<Utc> = std::time::SystemTime::now().into();
        let last_week = now - Days::new(7);
        let result1 = database.get_num_votes_for_user_since(TEST_USER_ID, &last_week);
        assert!(result1.is_ok());
        assert_eq!(result1.unwrap(), 0);

        let result2 = database.get_snapshot();
        assert!(result2.is_ok());
        let snapshot0 = result2.unwrap();
        assert!(!snapshot0.etag().token.is_empty());
        assert_eq!(snapshot0.ranking().len(), 2);
        assert!((snapshot0.ranking()[0].elo() - 1200.0).abs() < EPSILON);
        assert!((snapshot0.ranking()[1].elo() - 1200.0).abs() < EPSILON);

        let old_etag = snapshot0.etag().clone();
        let now: DateTime<Utc> = std::time::SystemTime::now().into();
        let result3 = database.add_vote_and_update_ratings(
            &old_etag,
            &Vote::new(
                TEST_USER_ID,
                now,
                t0.id().clone(),
                t1.id().clone(),
                Outcome::P1Win,
            ),
            &Rating::with_elo(t0.id().clone(), 1184.0),
            &Rating::with_elo(t1.id().clone(), 1216.0),
        );
        assert!(result3.is_ok());

        let result4 = database.get_snapshot();
        assert!(result4.is_ok());
        let snapshot1 = result4.unwrap();
        assert!(!snapshot1.etag().token.is_empty());
        assert_ne!(snapshot1.etag().token, old_etag.token);
        assert_eq!(snapshot1.ranking().len(), 2);
        if snapshot1.ranking()[0].task() == t0.id() {
            assert!((snapshot1.ranking()[0].elo() - 1184.0).abs() < EPSILON);
            assert!((snapshot1.ranking()[1].elo() - 1216.0).abs() < EPSILON);
        } else {
            assert!((snapshot1.ranking()[0].elo() - 1216.0).abs() < EPSILON);
            assert!((snapshot1.ranking()[1].elo() - 1184.0).abs() < EPSILON);
        }

        // Check the optimistic concurrency handling. We provide the old etag
        // and this should be rejected.
        let result5 = database.add_vote_and_update_ratings(
            &old_etag,
            &Vote::new(
                TEST_USER_ID,
                now,
                t0.id().clone(),
                t1.id().clone(),
                Outcome::P1Win,
            ),
            &Rating::with_elo(t0.id().clone(), 1184.0),
            &Rating::with_elo(t1.id().clone(), 1216.0),
        );
        assert!(result5.is_err());
        assert_eq!(
            result5.err().unwrap().code(),
            ErrorCode::OptimisticConcurrencyRetryTransaction
        );
    }
}
