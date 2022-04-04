use anyhow::Result;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::io;
use terra_rust_api::core_types::Coin;

#[derive(Debug, Default)]
pub struct FeeSummary {
    pub count: usize,
    pub fees: HashMap<String, Decimal>,
}
impl FeeSummary {
    pub fn add_fee(&mut self, coin: &Coin) -> &mut Self {
        self.fees
            .entry(coin.denom.clone())
            .and_modify(|f| *f += coin.amount)
            .or_insert(coin.amount);
        self.count += 1;
        self
    }

    pub fn create(coin: &Coin) -> Self {
        Self {
            count: 1,
            fees: HashMap::from([(coin.denom.clone(), coin.amount)]),
        }
    }
}
fn main() -> Result<()> {
    let mut buf: String = String::new();
    let mut start: Option<u64> = None;
    let mut last: Option<u64> = None;
    let mut fees: HashMap<String, FeeSummary> = Default::default();
    //let mut i = 0;
    while io::stdin().read_line(&mut buf)? > 0 {
        // The iterator yields Result<StringRecord, Error>, so we check the
        // error here.

        let parts = buf.trim().split(",").collect::<Vec<_>>();
        if start.is_none() {
            if let Some(blocknum) = parts.first() {
                if let Ok(num) = blocknum.parse::<u64>() {
                    start = Some(num)
                }
            }
        } else {
            //  println!("{:?} {:?}", start, parts);

            if parts.len() > 2 {
                let blocknum = parts.get(0).unwrap();
                let account = String::from(parts.get(1).unwrap().clone());
                let num = blocknum.parse::<u64>()?;
                last = Some(num);

                for coin_str in &parts.as_slice()[2..] {
                    if let Some(coin) = Coin::parse(coin_str)? {
                        fees.entry(account.clone())
                            .and_modify(|f| {
                                f.add_fee(&coin);
                            })
                            .or_insert(FeeSummary::create(&coin));
                    }
                }
            }

            // i += 1;
        }

        buf.clear();
    }
    println!("# Start,End,#Accounts");
    println!(
        "# {},{},{}",
        start.unwrap_or_default(),
        last.unwrap_or_default(),
        fees.len()
    );
    let mut fee_tally: HashMap<String, Decimal> = Default::default();

    for e in fees.values() {
        for fee_entry in &e.fees {
            fee_tally
                .entry(fee_entry.0.clone())
                .and_modify(|f| *f += fee_entry.1)
                .or_insert(fee_entry.1.clone());
        }
    }
    println!("# Totals");
    println!("denom,amount");
    for x in fee_tally.iter() {
        println!("{},{}", x.0, x.1);
    }
    println!("\nAccount,#Transactions,Fees");
    for e in fees.iter() {
        let fees =
            e.1.fees
                .iter()
                .map(|f| Coin::create(f.0, f.1.clone()).to_string())
                .collect::<Vec<_>>()
                .join(",");
        println!("{},{},\"{}\"", e.0, e.1.count, fees)
    }
    Ok(())
}
