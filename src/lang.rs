use std::fmt::{self, Display};
use std::rc::Rc;

use anyhow::Result;
use serde::Deserialize;

use crate::{buffer::BufferPoolManager, disk::PageId, query, table::Table, tuple};

#[derive(Debug, Clone, Deserialize)]
pub enum Request {
    CreateTable(CreateTableRequest),
    Insert(InsertRequest),
    Query(QueryRequest),
}

impl Request {
    pub fn execute(&self, bufmgr: &mut BufferPoolManager) -> Result<Response> {
        match self {
            Request::CreateTable(create_table) => {
                Ok(Response::CreateTable(create_table.execute(bufmgr)?))
            }
            Request::Insert(insert) => Ok(Response::Insert(insert.execute(bufmgr)?)),
            Request::Query(query) => Ok(Response::Query(query.execute(bufmgr)?)),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateTableRequest {
    num_key_elems: usize,
}

impl CreateTableRequest {
    fn execute(&self, bufmgr: &mut BufferPoolManager) -> Result<CreateTableResponse> {
        let mut table = Table {
            meta_page_id: PageId::INVALID_PAGE_ID,
            num_key_elems: self.num_key_elems,
            // TODO:
            unique_indices: vec![],
        };
        table.create(bufmgr)?;
        Ok(CreateTableResponse {
            table_page_id: table.meta_page_id.to_u64(),
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct InsertRequest {
    table: u64,
    num_key_elems: usize,
    record: Vec<String>,
}

impl InsertRequest {
    fn execute(&self, bufmgr: &mut BufferPoolManager) -> Result<InsertResponse> {
        let table = Table {
            meta_page_id: PageId(self.table),
            num_key_elems: self.num_key_elems,
            // TODO:
            unique_indices: vec![],
        };
        table.insert(
            bufmgr,
            self.record
                .iter()
                .map(String::as_bytes)
                .collect::<Vec<_>>()
                .as_slice(),
        )?;
        Ok(InsertResponse)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct QueryRequest {
    #[serde(flatten)]
    plan_node: PlanNode,
}

impl QueryRequest {
    fn execute(&self, bufmgr: &mut BufferPoolManager) -> Result<QueryResponse> {
        let plan = self.plan_node.build_plan_node();
        let mut exec = plan.start(bufmgr)?;
        let mut records = vec![];
        while let Some(record) = exec.next(bufmgr)? {
            records.push(record);
        }
        Ok(QueryResponse { records })
    }
}

#[derive(Debug, Clone, Deserialize)]
enum PlanNode {
    SeqScan(SeqScanPlan),
    Filter(FilterPlan),
}

impl PlanNode {
    fn build_plan_node(&self) -> Box<dyn query::PlanNode> {
        match self {
            PlanNode::SeqScan(seq_scan) => Box::new(seq_scan.build_plan_node()),
            PlanNode::Filter(filter) => Box::new(filter.build_plan_node()),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct SeqScanPlan {
    table: u64,
    #[serde(default)]
    key: Option<Vec<String>>,
    #[serde(default, rename = "while")]
    while_expr: WhileExpr,
}

impl SeqScanPlan {
    fn build_plan_node(&self) -> query::SeqScan {
        let while_expr = self.while_expr.clone();
        query::SeqScan {
            table_meta_page_id: PageId(self.table),
            search_mode: match &self.key {
                Some(key) => query::TupleSearchMode::Key(
                    key.iter()
                        .map(String::as_bytes)
                        .map(|s| s.to_vec())
                        .collect(),
                ),
                None => query::TupleSearchMode::Start,
            },
            while_cond: Rc::new(move |key| while_expr.eval(key)),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct FilterPlan {
    #[serde(rename = "where")]
    where_expr: BoolExpr,
    from: Box<PlanNode>,
}

impl FilterPlan {
    fn build_plan_node(&self) -> query::Filter {
        let where_expr = self.where_expr.clone();
        query::Filter {
            inner_plan: self.from.build_plan_node(),
            cond: Rc::new(move |record| where_expr.eval(record)),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
enum WhileExpr {
    True,
    Eq(Vec<String>),
    Lt(Vec<String>),
    Lte(Vec<String>),
    Gt(Vec<String>),
    Gte(Vec<String>),
}

impl Default for WhileExpr {
    fn default() -> Self {
        WhileExpr::True
    }
}

impl WhileExpr {
    fn eval(&self, key: query::TupleSlice) -> bool {
        use std::cmp::Ordering;
        let cmp = |other: &Vec<String>| {
            key.iter()
                .map(Vec::as_slice)
                .cmp(other.iter().map(String::as_bytes))
        };
        match self {
            WhileExpr::True => true,
            WhileExpr::Eq(other) => cmp(other) == Ordering::Equal,
            WhileExpr::Lt(other) => cmp(other) == Ordering::Less,
            WhileExpr::Lte(other) => cmp(other) == Ordering::Less || cmp(other) == Ordering::Equal,
            WhileExpr::Gt(other) => cmp(other) == Ordering::Greater,
            WhileExpr::Gte(other) => {
                cmp(other) == Ordering::Greater || cmp(other) == Ordering::Equal
            }
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
enum BytesExpr {
    Literal(String),
    Column(usize),
}

impl BytesExpr {
    fn eval<'a: 'b, 'b>(&'a self, record: query::TupleSlice<'b>) -> &'b [u8] {
        match self {
            BytesExpr::Literal(literal) => literal.as_bytes(),
            BytesExpr::Column(idx) => &record[*idx],
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
enum BoolExpr {
    True,
    False,
    And(Box<BoolExpr>, Box<BoolExpr>),
    Or(Box<BoolExpr>, Box<BoolExpr>),
    Not(Box<BoolExpr>),
    Eq(BytesExpr, BytesExpr),
    Lt(BytesExpr, BytesExpr),
    Lte(BytesExpr, BytesExpr),
    Gt(BytesExpr, BytesExpr),
    Gte(BytesExpr, BytesExpr),
}

impl BoolExpr {
    fn eval(&self, record: query::TupleSlice) -> bool {
        match self {
            BoolExpr::True => true,
            BoolExpr::False => false,
            BoolExpr::And(l, r) => l.eval(record) && r.eval(record),
            BoolExpr::Or(l, r) => l.eval(record) || r.eval(record),
            BoolExpr::Not(e) => !e.eval(record),
            BoolExpr::Eq(l, r) => l.eval(record) == r.eval(record),
            BoolExpr::Lt(l, r) => l.eval(record) < r.eval(record),
            BoolExpr::Lte(l, r) => l.eval(record) <= r.eval(record),
            BoolExpr::Gt(l, r) => l.eval(record) > r.eval(record),
            BoolExpr::Gte(l, r) => l.eval(record) >= r.eval(record),
        }
    }
}

#[derive(Debug)]
pub enum Response {
    CreateTable(CreateTableResponse),
    Insert(InsertResponse),
    Query(QueryResponse),
}

impl Display for Response {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Response::CreateTable(create_table) => create_table.fmt(f),
            Response::Insert(insert) => insert.fmt(f),
            Response::Query(query) => query.fmt(f),
        }
    }
}

#[derive(Debug)]
pub struct CreateTableResponse {
    table_page_id: u64,
}

impl Display for CreateTableResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "table_page_id = {}", self.table_page_id)
    }
}

#[derive(Debug)]
pub struct InsertResponse;

impl Display for InsertResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "")
    }
}

#[derive(Debug)]
pub struct QueryResponse {
    records: Vec<query::Tuple>,
}

impl Display for QueryResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for record in &self.records {
            writeln!(f, "{:?}", tuple::Pretty(&record))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bool_expr() {
        use BoolExpr::*;
        use BytesExpr::*;
        let record = &[];
        let a = || Literal("A".to_string());
        let b = || Literal("B".to_string());

        assert_eq!(And(Box::new(True), Box::new(True)).eval(record), true);
        assert_eq!(And(Box::new(True), Box::new(False)).eval(record), false);

        assert_eq!(Or(Box::new(False), Box::new(False)).eval(record), false);
        assert_eq!(Or(Box::new(False), Box::new(True)).eval(record), true);

        assert_eq!(Not(Box::new(False)).eval(record), true);
        assert_eq!(Not(Box::new(True)).eval(record), false);

        assert_eq!(Eq(a(), a()).eval(record), true);
        assert_eq!(Eq(a(), b()).eval(record), false);

        assert_eq!(Lt(a(), b()).eval(record), true);
        assert_eq!(Lt(a(), a()).eval(record), false);
        assert_eq!(Lt(b(), a()).eval(record), false);

        assert_eq!(Lte(a(), a()).eval(record), true);
        assert_eq!(Lte(a(), b()).eval(record), true);
        assert_eq!(Lte(b(), a()).eval(record), false);

        assert_eq!(Gt(b(), a()).eval(record), true);
        assert_eq!(Gt(a(), a()).eval(record), false);
        assert_eq!(Gt(a(), b()).eval(record), false);

        assert_eq!(Gte(a(), a()).eval(record), true);
        assert_eq!(Gte(b(), a()).eval(record), true);
        assert_eq!(Gte(a(), b()).eval(record), false);
    }

    #[test]
    fn test_build_plan_node() -> anyhow::Result<()> {
        use tempfile::tempfile;

        use crate::buffer::{BufferPool, BufferPoolManager};
        use crate::disk::DiskManager;

        let disk = DiskManager::new(tempfile()?)?;
        let pool = BufferPool::new(10);
        let mut bufmgr = BufferPoolManager::new(disk, pool);

        let mut table = Table {
            meta_page_id: PageId::INVALID_PAGE_ID,
            num_key_elems: 1,
            unique_indices: vec![],
        };
        table.create(&mut bufmgr)?;
        table.insert(&mut bufmgr, &[b"z", b"Alice", b"Smith"])?;
        table.insert(&mut bufmgr, &[b"x", b"Bob", b"Johnson"])?;
        table.insert(&mut bufmgr, &[b"y", b"Charlie", b"Williams"])?;
        table.insert(&mut bufmgr, &[b"w", b"Dave", b"Miller"])?;
        table.insert(&mut bufmgr, &[b"v", b"Eve", b"Brown"])?;

        let query = PlanNode::Filter(FilterPlan {
            where_expr: BoolExpr::Lt(BytesExpr::Column(1), BytesExpr::Literal("Dave".to_string())),
            from: Box::new(PlanNode::SeqScan(SeqScanPlan {
                table: table.meta_page_id.to_u64(),
                key: Some(vec!["w".to_string()]),
                while_expr: WhileExpr::Lt(vec!["z".to_string()]),
            })),
        });
        let plan = query.build_plan_node();
        let mut exec = plan.start(&mut bufmgr)?;
        assert_eq!(exec.next(&mut bufmgr)?, Some(vec![b"x".to_vec(), b"Bob".to_vec(), b"Johnson".to_vec()]));
        assert_eq!(exec.next(&mut bufmgr)?, Some(vec![b"y".to_vec(), b"Charlie".to_vec(), b"Williams".to_vec()]));
        assert_eq!(exec.next(&mut bufmgr)?, None);
        Ok(())
    }
}
