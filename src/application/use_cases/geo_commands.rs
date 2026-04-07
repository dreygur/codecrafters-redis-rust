use std::sync::Arc;

use crate::application::ports::StorePort;
use crate::domain::DomainError;
use crate::infrastructure::geo::GeoUtils;
use crate::infrastructure::networking::resp::RespEncoder;
use bytes::Bytes;

pub struct GeoCommands {
    store: Arc<dyn StorePort>,
}

impl GeoCommands {
    pub fn new(store: Arc<dyn StorePort>) -> Self {
        Self { store }
    }

    pub fn geoadd(&self, args: &[String]) -> Bytes {
        if args.len() < 5 || (args.len() - 2) % 3 != 0 {
            return RespEncoder::error(&DomainError::WrongArgCount.to_string());
        }
        let key = &args[1];
        let mut added = 0i64;
        for chunk in args[2..].chunks(3) {
            let Ok(lon) = chunk[0].parse::<f64>() else {
                return RespEncoder::error("value is not a valid float");
            };
            let Ok(lat) = chunk[1].parse::<f64>() else {
                return RespEncoder::error("value is not a valid float");
            };
            if !GeoUtils::validate(lon, lat) {
                return RespEncoder::error("invalid longitude,latitude pair");
            }
            if self.store.geoadd(key, lon, lat, chunk[2].clone()) {
                added += 1;
            }
        }
        RespEncoder::integer(added)
    }

    pub fn geopos(&self, args: &[String]) -> Bytes {
        if args.len() < 3 {
            return RespEncoder::error(&DomainError::WrongArgCount.to_string());
        }
        let key = &args[1];
        let items = args[2..]
            .iter()
            .map(|member| match self.store.geopos(key, member) {
                Some((lon, lat)) => RespEncoder::array(vec![
                    RespEncoder::bulk_string(&format!("{lon}")),
                    RespEncoder::bulk_string(&format!("{lat}")),
                ]),
                None => RespEncoder::null_array(),
            })
            .collect();
        RespEncoder::array(items)
    }

    pub fn geodist(&self, args: &[String]) -> Bytes {
        if args.len() < 4 {
            return RespEncoder::error(&DomainError::WrongArgCount.to_string());
        }
        let unit = args.get(4).map(String::as_str).unwrap_or("m");
        match self.store.geodist(&args[1], &args[2], &args[3]) {
            Some(dist_m) => RespEncoder::bulk_string(&format!("{:.4}", GeoUtils::from_metres(dist_m, unit))),
            None => RespEncoder::null_bulk(),
        }
    }

    pub fn geosearch(&self, args: &[String]) -> Bytes {
        if args.len() < 7 {
            return RespEncoder::error(&DomainError::WrongArgCount.to_string());
        }
        let key = &args[1];
        let mut i = 2;

        let (center_lon, center_lat) = match args[i].to_uppercase().as_str() {
            "FROMLONLAT" => {
                let Ok(lon) = args[i + 1].parse::<f64>() else {
                    return RespEncoder::error("value is not a valid float");
                };
                let Ok(lat) = args[i + 2].parse::<f64>() else {
                    return RespEncoder::error("value is not a valid float");
                };
                i += 3;
                (lon, lat)
            }
            "FROMMEMBER" => {
                let Some(pos) = self.store.geopos(key, &args[i + 1]) else {
                    return RespEncoder::error("could not find the requested member");
                };
                i += 2;
                pos
            }
            _ => return RespEncoder::error("syntax error"),
        };

        if args.get(i).map(|s| s.to_uppercase()).as_deref() != Some("BYRADIUS") {
            return RespEncoder::error("syntax error");
        }

        let Ok(radius) = args[i + 1].parse::<f64>() else {
            return RespEncoder::error("value is not a valid float");
        };
        let radius_m = GeoUtils::to_metres(radius, args[i + 2].as_str());
        i += 3;

        let descending = args.get(i).map(|s| s.to_uppercase() == "DESC").unwrap_or(false);
        let mut hits = self.store.geosearch_radius(key, center_lon, center_lat, radius_m);

        if descending {
            hits.sort_by(|a, b| b.1.total_cmp(&a.1));
        } else {
            hits.sort_by(|a, b| a.1.total_cmp(&b.1));
        }

        RespEncoder::array(hits.into_iter().map(|(m, _)| RespEncoder::bulk_string(&m)).collect())
    }
}
