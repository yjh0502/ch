use anyhow::Result;
use ordslice::Ext;
use s2::{
    cap::Cap,
    cellid::CellID,
    latlng::LatLng,
    point::Point,
    region::RegionCoverer,
    s1::{Angle, Deg, Rad},
};
use std::collections::HashSet;

use super::*;

pub struct Network {
    points: Vec<CellID>,
    edges: Vec<Edge>,
}

#[derive(PartialOrd, Ord, PartialEq, Eq)]
pub struct Edge {
    pub s: u32,
    pub t: u32,
    pub len: u32,
}

fn angle_to_km(angle: Angle) -> f64 {
    angle.rad() * 6371.01
}

fn km_to_angle(km: f64) -> Angle {
    Angle::from(Rad(km / 6371.01))
}

impl Network {
    pub fn from_path<P>(p: P) -> Result<Network>
    where
        P: AsRef<Path>,
    {
        let mut reader = shapefile::ShapeReader::from_path(p)?;

        let mut total_dist = 0usize;
        let mut record_count = 0;
        let mut duplicate_count = 0;

        // collect points
        let mut points = HashSet::new();
        let mut edges = Vec::new();
        for result in reader.iter_shapes() {
            let shape = result?;
            record_count += 1;

            let pl = match shape {
                shapefile::Shape::Polyline(pl) => pl,
                _ => todo!(),
            };

            for part in pl.parts() {
                let mut last: Option<(CellID, LatLng)> = None;
                for point in part {
                    let y = Deg(point.y);
                    let x = Deg(point.x);
                    let ll = LatLng::new(Angle::from(y), Angle::from(x));
                    let cell = CellID::from(ll);

                    if !points.insert(cell) {
                        duplicate_count += 1;
                    }
                    if let Some((last, last_ll)) = last {
                        // distance in centimeter
                        let len = angle_to_km(last_ll.distance(&ll)) * 1000.0 * 100.0;
                        let len = len as u32;
                        total_dist += len as usize;
                        edges.push((last, cell, len));
                    }
                    last = Some((cell, ll));
                }
            }
        }

        // sort points
        let points = {
            let mut points = points.into_iter().collect::<Vec<_>>();
            points.sort();
            points
        };

        let mut edges = edges
            .into_par_iter()
            .flat_map(|(s, t, len)| {
                let s = points.binary_search(&s).unwrap() as u32;
                let t = points.binary_search(&t).unwrap() as u32;

                [Edge { s, t, len }, Edge { s: t, t: s, len }]
            })
            .collect::<Vec<_>>();

        eprintln!(
            "records={}, edges={}, points={}, dups={}, dist={}km, avg={}m",
            record_count,
            edges.len(),
            points.len(),
            duplicate_count,
            total_dist / (100 * 1000),
            total_dist / 100 / edges.len(),
        );

        edges.sort();

        Ok(Self { points, edges })
    }

    pub fn nearest(&self, lat: f64, lng: f64, dist_km: f64) -> Option<u32> {
        let ll = LatLng::new(Angle::from(Deg(lat)), Angle::from(Deg(lng)));
        let p = Point::from(ll);

        let a = km_to_angle(dist_km);
        let cap = Cap::from_center_angle(&p, &a);

        let cov = RegionCoverer {
            min_level: 14,
            max_level: 16,
            level_mod: 1,
            max_cells: 100,
        };

        let cu = cov.covering(&cap);
        for cell_id in &cu.0 {
            let level = cell_id.level();
            let r = self
                .points
                .equal_range_by(|p| p.parent(level).cmp(&cell_id));
            if r.len() > 0 {
                return Some(r.start as u32);
            }
        }

        None
    }

    pub fn point(&self, id: u32) -> CellID {
        self.points[id as usize]
    }

    fn links(&self, id: u32) -> &[Edge] {
        let r = self.edges.equal_range_by(|e| e.s.cmp(&id));
        &self.edges[r]
    }
}

impl<'a> From<&'a Network> for Graph {
    fn from(network: &'a Network) -> Self {
        let mut idx_links = Vec::with_capacity(network.points.len());

        for i in 0..network.points.len() {
            let links = network.links(i as u32);
            idx_links.push(
                links
                    .into_iter()
                    .map(|link| {
                        let idx = IdxNodeKey(link.t);
                        let cost = link.len;
                        IdxLink::new(idx, cost, IdxLinkDir::Forward)
                    })
                    .collect::<Vec<_>>(),
            );
        }

        Self::from_links(idx_links)
    }
}
