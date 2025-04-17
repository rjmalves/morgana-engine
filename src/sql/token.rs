use std::fmt::{self, Display};

#[derive(PartialEq)]
pub(crate) enum Token {
    Keyword(Keyword),
    Identifier(String),
    Separator(Separator),
    // decide if this separation means something here
    String(String),
    Integer(String),
    Real(String),
    Datetime(String),
    Equal,
    NotEqual,
    LessThan,
    GreaterThan,
    LessThanOrEqual,
    GreaterThanOrEqual,
    Plus,
    Minus,
    Multiply,
    Divide,
    LeftParenthesis,
    RightParenthesis,
    Comma,
    Semicolon,
    EndOfFile,
}

impl Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Keyword(keyword) => write!(f, "{keyword}"),
            Self::Identifier(identifier) => write!(f, "{identifier}"),
            Self::Separator(separator) => write!(f, "{separator}"),
            Self::String(string) => write!(f, "{string}"),
            Self::Integer(integer) => write!(f, "{integer}"),
            Self::Real(real) => write!(f, "{real}"),
            Self::Datetime(datetime) => write!(f, "{datetime}"),
            Self::Equal => f.write_str("="),
            Self::NotEqual => f.write_str("!="),
            Self::LessThan => f.write_str("<"),
            Self::GreaterThan => f.write_str(">"),
            Self::LessThanOrEqual => f.write_str("<="),
            Self::GreaterThanOrEqual => f.write_str(">="),
            Self::Plus => f.write_str("+"),
            Self::Minus => f.write_str("-"),
            Self::Multiply => f.write_str("*"),
            Self::Divide => f.write_str("/"),
            Self::LeftParenthesis => f.write_str("("),
            Self::RightParenthesis => f.write_str(")"),
            Self::Comma => f.write_str(","),
            Self::Semicolon => f.write_str(";"),
            Self::EndOfFile => f.write_str("EOF"),
        }
    }
}

#[derive(PartialEq, Clone, Copy)]
pub(crate) enum Keyword {
    Select,
    Group,
    Order,
    By,
    Inner,
    Outer,
    Left,
    Right,
    Join,
    On,
}

impl Display for Keyword {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            Self::Select => "SELECT",
            Self::Group => "GROUP",
            Self::Order => "ORDER",
            Self::By => "BY",
            Self::Inner => "INNER",
            Self::Outer => "OUTER",
            Self::Left => "LEFT",
            Self::Right => "RIGHT",
            Self::Join => "JOIN",
            Self::On => "ON",
        })
    }
}

#[derive(PartialEq)]
pub(crate) enum Separator {
    Space,
    Tab,
    LineBreak,
    CarriageReturn,
}

impl Display for Separator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            Self::Space => " ",
            Self::Tab => "\t",
            Self::LineBreak => "\n",
            Self::CarriageReturn => "",
        })
    }
}
