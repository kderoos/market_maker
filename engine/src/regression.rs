extern crate nalgebra as na;
use std::ops::Mul;

use na::{DMatrix, DVector};

// Penetration of trade price into the orderbook decays exponentially with distance from mid-price: C(x) = A*exp(-k*x).
// We fit log(C(x)) = log(A) - k*x using simple linear regression to estimate A and k. 
// Writing y = log(C(x)), we have y = beta0 + beta1*x + err, where beta0 = log(A), beta1 = -k.

pub trait RegressionEngine: Send + Sync {
    fn fit(&mut self) -> Result<DVector<f64>, String>;// (log(A), -k)
}
pub struct SimpleSLR{
    //We fit the linear model y = X * beta + err (or yi = beta0 + beta1*xi + err_i)
    pub Y: DVector<f64>,
    pub X: DMatrix<f64>,
    pub beta: Option<DVector<f64>>,
}
impl RegressionEngine for SimpleSLR {
    // fn from_xy(X: DMatrix<f64>, Y: DVector<f64>) -> Self {
    //     Self {X, Y, beta: None }
    // }
    fn fit(&mut self) -> Result<DVector<f64>, String> {
        // beta = (X'X)^-1 X'Y
        let xt = self.X.transpose();
        let xtx = &xt * &self.X;
        let Some(xtx_inv) = xtx.try_inverse() else {
            return Err("X'X matrix is singular, cannot invert".to_string());
        };
        let xty = xt *&self.Y;
        let beta = xtx_inv.mul(&xty);
        self.beta = Some(beta.clone());
        Ok(beta)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_lsr_linear() {
        // y = 3x + 5, for x = 0 .. 10
        let x_data = (0..=10).map(|x| x as f64).collect::<Vec<f64>>();
        let n = x_data.len();

        let mut x_mat = DMatrix::from_element(n,2,1.0); // 2xn matrix with 1.0
        let y_mat = DVector::from_fn(n,|i,_| (3*i+5) as f64);
        for (i,v) in x_data.iter().enumerate() {
            let f = i as f64;
            x_mat[(i,1)] = f;
        }
        
        print!("X:{} Y:{}",x_mat.to_string(),y_mat.to_string());

        let mut slr = SimpleSLR{ X: x_mat, Y: y_mat, beta: None };
        match slr.fit() {
            Ok(beta) => {
                println!("Fitted beta: {:?}", beta);
                assert_eq!(beta[0],5.0);
                assert_eq!(beta[1],3.0);
            }
            Err(e) => {
                println!("SLR fit failed: {}", e);
                assert!(false);
            }
        }

    }
}