use chrono::prelude::*;

/// Expressions used in select, update, delete and insert statements.
#[derive(Debug, PartialEq, Clone)]
pub(crate) enum Expression {
    Identifier(String),

    Value(Value),

    Wildcard,

    BinaryOperation {
        left: Box<Self>,
        operator: BinaryOperator,
        right: Box<Self>,
    },

    UnaryOperation {
        operator: UnaryOperator,
        expr: Box<Self>,
    },

    Nested(Box<Self>),
}

/// Resolved values from expressions.
#[derive(Debug, PartialEq, Clone)]
pub enum Value {
    /// UTF-8 string.
    String(String),

    /// UTC Datetime.
    Datetime(DateTime<Utc>),

    /// Boolean, true or false.
    Bool(bool),

    /// Generic integer type.
    Integer(i64),

    /// Generic real type.
    Real(f64),
}

/// Binary operators used in expressions.
#[derive(Debug, PartialEq, Clone, Copy)]
pub(crate) enum BinaryOperator {
    Eq,
    Neq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Plus,
    Minus,
    Mul,
    Div,
    And,
    Or,
}

/// Unary operators used in expressions.
#[derive(Debug, PartialEq, Clone, Copy)]
pub(crate) enum UnaryOperator {
    Plus,
    Minus,
}
