use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
pub struct BackoffCfg {
    pub initial_seconds: f64,
    pub max_seconds: f64,
    pub multiplier: f64,
    pub jitter: f64,
    pub max_attempts: u32,
}

impl Default for BackoffCfg {
    fn default() -> Self {
        Self {
            initial_seconds: 1.0,
            max_seconds: 300.0,
            multiplier: 2.0,
            jitter: 0.2,
            max_attempts: 12,
        }
    }
}

impl BackoffCfg {
    pub fn delay_seconds(&self, attempts: u32, jitter_roll: f64) -> f64 {
        let base =
            (self.initial_seconds * self.multiplier.powi(attempts as i32)).min(self.max_seconds);
        (base * (1.0 + jitter_roll * self.jitter)).max(0.0)
    }

    pub fn gave_up(&self, attempts: u32) -> bool {
        self.max_attempts > 0 && attempts >= self.max_attempts
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doubles_then_caps() {
        let cfg = BackoffCfg::default();
        assert_eq!(cfg.delay_seconds(0, 0.0), 1.0);
        assert_eq!(cfg.delay_seconds(1, 0.0), 2.0);
        assert_eq!(cfg.delay_seconds(4, 0.0), 16.0);
        assert_eq!(cfg.delay_seconds(20, 0.0), 300.0);
    }

    #[test]
    fn jitter_bounds() {
        let cfg = BackoffCfg::default();
        assert_eq!(cfg.delay_seconds(0, 1.0), 1.2);
        assert_eq!(cfg.delay_seconds(0, -1.0), 0.8);
    }

    #[test]
    fn gives_up_at_cap_and_never_when_zero() {
        let cfg = BackoffCfg { max_attempts: 3, ..Default::default() };
        assert!(!cfg.gave_up(2));
        assert!(cfg.gave_up(3));
        assert!(cfg.gave_up(9));
        let forever = BackoffCfg { max_attempts: 0, ..Default::default() };
        assert!(!forever.gave_up(1_000_000));
    }
}
