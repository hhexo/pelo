use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

use crate::elo::Outcome;

use std::fmt;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    id: String,
    limit_votes_per_week: i32,
}
impl User {
    pub fn new(id: &str, limit_votes_per_week: i32) -> Self {
        User {
            id: id.to_string(),
            limit_votes_per_week: limit_votes_per_week,
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }
    pub fn limit_votes_per_week(&self) -> i32 {
        self.limit_votes_per_week
    }
    pub fn is_limited(&self) -> bool {
        self.limit_votes_per_week >= 0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Vote {
    voter: String,
    time: DateTime<Utc>,
    task0: Uuid,
    task1: Uuid,
    outcome: Outcome,
}
impl Vote {
    pub fn new(
        voter: &str,
        time: DateTime<Utc>,
        task0: Uuid,
        task1: Uuid,
        outcome: Outcome,
    ) -> Self {
        Vote {
            voter: voter.to_string(),
            time: time,
            task0: task0,
            task1: task1,
            outcome: outcome,
        }
    }

    pub fn voter(&self) -> &str {
        &self.voter
    }
    pub fn time(&self) -> &DateTime<Utc> {
        &self.time
    }
    pub fn task0(&self) -> &Uuid {
        &self.task0
    }
    pub fn task1(&self) -> &Uuid {
        &self.task1
    }
    pub fn outcome(&self) -> Outcome {
        self.outcome
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    id: Uuid,
    summary: String,
    link: Url,
    closed: bool,
}
impl Task {
    pub fn new(id: Uuid, summary: &str, link: Url, closed: bool) -> Self {
        Task {
            id: id,
            summary: summary.to_string(),
            link: link,
            closed: closed,
        }
    }

    pub fn id(&self) -> &Uuid {
        &self.id
    }
    pub fn summary(&self) -> &str {
        &self.summary
    }
    pub fn link(&self) -> &Url {
        &self.link
    }
    pub fn closed(&self) -> bool {
        self.closed
    }
    pub fn close(&mut self) {
        self.closed = true;
    }
}
impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}{}: {} | {})",
            if self.closed { "(closed) " } else { "" },
            self.id,
            self.summary,
            self.link
        )
    }
}

const DEFAULT_START_RATING: f32 = 1200.0;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rating {
    task: Uuid,
    elo: f32,
}
impl Rating {
    pub fn new(task: Uuid) -> Self {
        Rating::with_elo(task, DEFAULT_START_RATING)
    }

    pub fn with_elo(task: Uuid, elo: f32) -> Self {
        Rating {
            task: task,
            elo: elo,
        }
    }

    pub fn task(&self) -> &Uuid {
        &self.task
    }
    pub fn elo(&self) -> f32 {
        self.elo
    }
}
