use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Outcome {
    P0Win,
    Draw,
    P1Win,
}

const K: f32 = 32.0;

fn q(elo: f32) -> f32 {
    10.0_f32.powf(elo / 400.0)
}

fn e(own_elo: f32, opponent_elo: f32) -> f32 {
    q(own_elo) / (q(own_elo) + q(opponent_elo))
}

fn s(outcome: Outcome) -> f32 {
    match outcome {
        Outcome::P0Win => 0.0,
        Outcome::Draw => 0.5,
        Outcome::P1Win => 1.0,
    }
}

pub fn new_elo_pair(p0_elo: f32, p1_elo: f32, outcome: Outcome) -> (f32, f32) {
    (
        p0_elo + K * ((1.0 - s(outcome)) - e(p0_elo, p1_elo)),
        p1_elo + K * (s(outcome) - e(p1_elo, p0_elo)),
    )
}

#[cfg(test)]
mod tests {
    use crate::elo::{new_elo_pair, Outcome};

    const EPSILON: f32 = 0.000001;

    #[test]
    fn test_elo_calculation_loss() {
        let (e0_new, e1_new) = new_elo_pair(1200.0, 800.0, Outcome::P1Win);
        assert!((e0_new - 1170.90909090).abs() < EPSILON);
        assert!((e1_new - 829.09090909).abs() < EPSILON);
    }

    #[test]
    fn test_elo_calculation_draw() {
        let (e0_new, e1_new) = new_elo_pair(1200.0, 800.0, Outcome::Draw);
        assert!((e0_new - 1186.90909090).abs() < EPSILON);
        assert!((e1_new - 813.09090909).abs() < EPSILON);
    }

    #[test]
    fn test_elo_calculation_win() {
        let (e0_new, e1_new) = new_elo_pair(1200.0, 800.0, Outcome::P0Win);
        assert!((e0_new - 1202.90909090).abs() < EPSILON);
        assert!((e1_new - 797.09090909).abs() < EPSILON);
    }
}
