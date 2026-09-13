//! One property encoder shared by canonical and caller-ordered packet writers.
use super::{len_len, write_mqtt_bytes, write_mqtt_string, write_remaining_length, Error};
use bytes::{BufMut, BytesMut};

pub(super) enum Property<'a> {
    Byte(u8),
    Short(u16),
    Integer(u32),
    Variable(usize),
    String(&'a str),
    Binary(&'a [u8]),
    Pair(&'a str, &'a str),
}
impl Property<'_> {
    fn size(&self) -> Result<usize, Error> {
        fn text(s: &str) -> Result<usize, Error> {
            if s.len() > u16::MAX as usize || s.contains('\0') {
                return Err(Error::MalformedPacket);
            }
            Ok(2 + s.len())
        }
        Ok(match self {
            Self::Byte(_) => 1,
            Self::Short(_) => 2,
            Self::Integer(_) => 4,
            Self::Variable(v) if *v <= 268435455 => len_len(*v),
            Self::Variable(_) => return Err(Error::MalformedPacket),
            Self::String(s) => text(s)?,
            Self::Binary(b) if b.len() <= u16::MAX as usize => 2 + b.len(),
            Self::Binary(_) => return Err(Error::MalformedPacket),
            Self::Pair(k, v) => text(k)? + text(v)?,
        })
    }
    fn write(&self, buffer: &mut BytesMut) -> Result<(), Error> {
        match self {
            Self::Byte(v) => buffer.put_u8(*v),
            Self::Short(v) => buffer.put_u16(*v),
            Self::Integer(v) => buffer.put_u32(*v),
            Self::Variable(v) => {
                write_remaining_length(buffer, *v)?;
            }
            Self::String(v) => write_mqtt_string(buffer, v),
            Self::Binary(v) => write_mqtt_bytes(buffer, v),
            Self::Pair(k, v) => {
                write_mqtt_string(buffer, k);
                write_mqtt_string(buffer, v);
            }
        }
        Ok(())
    }
}
pub(super) fn write_properties(
    buffer: &mut BytesMut,
    values: Vec<(u8, Property<'_>)>,
    order: Option<&[u8]>,
) -> Result<(), Error> {
    if values.len() > 1024 {
        return Err(Error::MalformedPacket);
    }
    let length = values
        .iter()
        .try_fold(0usize, |n, (_, v)| Ok::<_, Error>(n + 1 + v.size()?))?;
    let mut indices = Vec::with_capacity(values.len());
    if let Some(order) = order {
        if order.len() != values.len() {
            return Err(Error::MalformedPacket);
        }
        let mut used = [false; 1024];
        for id in order {
            let index = values
                .iter()
                .enumerate()
                .position(|(i, (key, _))| !used[i] && key == id)
                .ok_or(Error::MalformedPacket)?;
            used[index] = true;
            indices.push(index);
        }
    } else {
        indices.extend(0..values.len());
    }
    write_remaining_length(buffer, length)?;
    for i in indices {
        let (id, value) = &values[i];
        buffer.put_u8(*id);
        value.write(buffer)?;
    }
    Ok(())
}
