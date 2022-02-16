// Concord
//
// Copyright (c) 2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0 License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

use std::collections::BTreeMap;
use std::error::Error;
use std::io::Cursor;
use std::io::Read;

use byteorder::{BigEndian, ReadBytesExt};

// TODO: Use of this duplicate crate has been identified as a potentially unnecessary dependency. We chould consider
//       what alternatives to using it are available without taking a dependency on a new crate and whether any of those
//       alternatives are worth actually using to avoid this dependency.
use duplicate::duplicate;

// TODO: Complete common serialization and deserialization code here to be included in all .rs files generated from CMF.

trait Serializable {
  fn serialize(&self, output: &mut Vec<u8>);
}

trait Deserializable {
  fn deserialize(&mut self, input: &mut Cursor<Vec<u8>>) -> Result<(), Box<dyn Error>>;
}

impl Serializable for bool {
  fn serialize(&self, output: &mut Vec<u8>) {
    let byte = if *self == true { 1 } else { 0 };
    output.push(byte);
    println!("{:?}", output);
  }
}

#[duplicate(int_type;
  [i8]; [i16]; [i32]; [i64];
  [u8]; [u16]; [u32]; [u64])]
impl Serializable for int_type {
  fn serialize(&self, output: &mut Vec<u8>) {
    output.extend_from_slice(&self.to_be_bytes());
    println!("{}", std::any::type_name::<int_type>());
  }
}

impl Serializable for String {
  fn serialize(&self, output: &mut Vec<u8>) {
    let t = self.as_bytes();
    let l : u32 = t.len().try_into().unwrap();

    l.serialize(output);
    output.extend_from_slice(&t);
    println!("{:?}", output);
  }
}

impl Deserializable for bool {
  fn deserialize(&mut self, input: &mut Cursor<Vec<u8>>) -> Result<(), Box<dyn Error>> {
    let value = input.read_u8()?;
    *self = if value == 1 { true } else { false };
    return Ok(());
  }
}

#[duplicate(
  int_type read_func;
  [i8]  [read_i8];
  [i16] [read_i16::<BigEndian>];
  [i32] [read_i32::<BigEndian>];
  [i64] [read_i64::<BigEndian>];

  [u8]  [read_u8];
  [u16] [read_u16::<BigEndian>];
  [u32] [read_u32::<BigEndian>];
  [u64] [read_u64::<BigEndian>];
)]
impl Deserializable for int_type {
  fn deserialize(&mut self, input: &mut Cursor<Vec<u8>>) -> Result<(), Box<dyn Error>> {
    *self = input.read_func()?;
    return Ok(());
  }
}

impl Deserializable for String {
  fn deserialize(&mut self, input: &mut Cursor<Vec<u8>>) -> Result<(), Box<dyn Error>> {
    let mut len : u32 = 0;
    len.deserialize(input)?;
    let mut adapter = input.take(len.into());

    let mut v = vec![0; len.try_into().unwrap()];
    adapter.read(&mut v);
    *self = String::from_utf8_lossy(&v).to_string();
    println!("{}", *self);
    return Ok(());
  }
}

impl<K,V> Serializable for (K, V) where K: Serializable, V: Serializable {
  fn serialize(&self, output: &mut Vec<u8>) {
    self.0.serialize(output);
    self.1.serialize(output);
  }
}

impl<K,V> Deserializable for (K, V) where K: Deserializable, V: Deserializable {
  fn deserialize(&mut self, input: &mut Cursor<Vec<u8>>) -> Result<(), Box<dyn Error>> {
    self.0.deserialize(input)?;
    self.1.deserialize(input)?;

    return Ok(());
  }
}
