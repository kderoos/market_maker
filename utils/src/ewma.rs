pub struct EwmaVariance {
    lambda: f64,
    var: Option<f64>,
}

impl EwmaVariance {
    pub fn new(interval_sec: f64, half_life_sec: f64) -> Self {
        let lambda = (-std::f64::consts::LN_2 * interval_sec / half_life_sec).exp();
        Self { lambda, var: None }
    }

    pub fn update(&mut self, sigma_raw: f64) -> f64 {
        let v = sigma_raw * sigma_raw;

        let new_var = match self.var {
            Some(prev) => self.lambda * prev + (1.0 - self.lambda) * v,
            None => v,
        };

        self.var = Some(new_var);
        new_var.sqrt()
    }
}
