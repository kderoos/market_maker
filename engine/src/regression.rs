extern crate nalgebra as na;
use std::ops::Mul;

use na::{DMatrix, DVector};

// Penetration of trade price into the orderbook decays exponentially with distance from mid-price: C(x) = A*exp(-k*x).
// We fit log(C(x)) = log(A) - k*x using simple linear regression to estimate A and k. 
// Writing y = log(C(x)), we have y = beta0 + beta1*x + err, where beta0 = log(A), beta1 = -k.

pub trait RegressionEngine: Send + Sync {
    fn fit(&mut self) -> DVector<f64>;// (log(A), -k)
}
pub struct SimpleSLR{
    //We fit the linear model y = X * beta + err (or yi = beta0 + beta1*xi + err_i)
    pub  Y: DVector<f64>,
    pub X: DMatrix<f64>,
    pub beta: Option<DVector<f64>>,
}
impl RegressionEngine for SimpleSLR {
    // fn from_xy(X: DMatrix<f64>, Y: DVector<f64>) -> Self {
    //     Self {X, Y, beta: None }
    // }
    fn fit(&mut self) -> DVector<f64> {
        // beta = (X'X)^-1 X'Y
        let xt = self.X.transpose();
        let xtx = &xt * &self.X;
        let xtx_inv = xtx.try_inverse().expect("RegressionEngine: Unable to invert X'X matrix");
        let xty = xt *&self.Y;
        let beta = xtx_inv.mul(&xty);
        self.beta = Some(beta.clone());
        beta    
    }
}
