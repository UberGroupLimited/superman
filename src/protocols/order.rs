use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "lowercase", tag = "type")]
pub enum Message {
	Complete(Complete),
	Update(Update),
	Progress(Progress),
	Print(Print),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Complete {
	#[serde(default = "null")]
	pub data: Value,

	#[serde(default = "null")]
	pub error: Value,

	#[serde(flatten)]
	pub others: HashMap<String, Value>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Update {
	#[serde(default = "null")]
	pub data: Value,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Progress {
	pub numerator: isize,
	pub denominator: isize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Print {
	pub content: String,
}

const fn null() -> Value {
	Value::Null
}
