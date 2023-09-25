use rand::{thread_rng, Rng};

fn main() {
    let n: u64 = thread_rng().gen_range(1..1_000_000);
    println!("{n} has {} prime factors", prime_factors(n).len());
}

fn prime_factors(n: u64) -> Vec<u64> {
    let mut factors = Vec::new();
    let mut n = n;
    let mut i = 2;
    while i * i <= n {
        while n % i == 0 {
            factors.push(i);
            n /= i;
        }
        i += 1;
    }
    if n > 1 {
        factors.push(n);
    }
    factors
}
