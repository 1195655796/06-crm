use std::{
    collections::HashSet,
    hash::{Hash, Hasher},
};

use anyhow::Result;
use chrono::{DateTime, Days, Utc};
use fake::{
    faker::{chrono::en::DateTimeBetween, internet::en::SafeEmail, name::zh_cn::Name},
    Dummy, Fake, Faker,
};
use nanoid::nanoid;
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json;
use sqlx::{
    error::BoxDynError,
    postgres::PgTypeInfo,
    postgres::{PgRow, PgValueRef, Postgres},
    Decode, Encode, Executor, FromRow, PgPool, Row, Type,
};
use std::fmt::{Display, Formatter};
use std::str::Utf8Error;
use tokio::time::Instant;
use uuid::Uuid;
use prost::Message;

// generate 10000 users and run them in a tx, repeat 500 times
#[derive(Default, Debug, Clone, Dummy, Serialize, Deserialize, PartialEq, Eq,)]
#[serde(rename_all = "lowercase")]
pub enum gender {
    #[default]
    female,
    male,
    unknown,
}

impl Type<Postgres> for gender {
    fn type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("gender")
    }
}

impl<'r> FromRow<'r, sqlx::postgres::PgRow> for gender {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        let gender_str: &str = row.get(0);
        match gender_str {
            "female" => Ok(gender::female),
            "male" => Ok(gender::male),
            "unknown" => Ok(gender::unknown),
            _ => Err(sqlx::Error::RowNotFound),
        }
    }
}

impl<'r> Decode<'r, Postgres> for gender {
    fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
        let gender_str = value
            .as_bytes()
            .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
            .map_err(BoxDynError::from)?;
        match gender_str.as_str() {
            "female" => Ok(gender::female),
            "male" => Ok(gender::male),
            "unknown" => Ok(gender::unknown),
            _ => Err(Box::new(sqlx::Error::RowNotFound)),
        }
    }
}

#[derive(Debug, Clone, Dummy, Serialize, Deserialize, PartialEq, Eq, FromRow)]
pub struct UserStat {
    #[dummy(faker = "UniqueEmail")]
    pub email: String,
    #[dummy(faker = "Name()")]
    pub name: String,
    pub gender: gender,
    #[dummy(faker = "DateTimeBetween(before(365*5), before(90))")]
    pub created_at: DateTime<Utc>,
    #[dummy(faker = "DateTimeBetween(before(30), now())")]
    pub last_visited_at: DateTime<Utc>,
    #[dummy(faker = "DateTimeBetween(before(90), now())")]
    pub last_watched_at: DateTime<Utc>,
    #[dummy(faker = "IntList(50, 100000, 100000)")]
    pub recent_watched: Vec<i32>,
    #[dummy(faker = "IntList(50, 200000, 100000)")]
    pub viewed_but_not_started: Vec<i32>,
    #[dummy(faker = "IntList(50, 300000, 100000)")]
    pub started_but_not_finished: Vec<i32>,
    #[dummy(faker = "IntList(50, 400000, 100000)")]
    pub finished: Vec<i32>,
    #[dummy(faker = "DateTimeBetween(before(45), now())")]
    pub last_email_notification: DateTime<Utc>,
    #[dummy(faker = "DateTimeBetween(before(15), now())")]
    pub last_in_app_notification: DateTime<Utc>,
    #[dummy(faker = "DateTimeBetween(before(90), now())")]
    pub last_sms_notification: DateTime<Utc>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let pool = PgPool::connect("postgres://postgres:postgres@localhost:5432/stats").await?;
    for i in 1..=500 {
        let users: HashSet<_> = (0..10000).map(|_| Faker.fake::<UserStat>()).collect();

        let start = Instant::now();
        raw_insert(users, &pool).await?;
        println!("Batch {} inserted in {:?}", i, start.elapsed());
    }
    Ok(())
}

async fn raw_insert(users: HashSet<UserStat>, pool: &PgPool) -> Result<()> {
    let mut sql = String::with_capacity(10 * 1000 * 1000);
    sql.push_str("
    INSERT INTO user_stats(email, name, created_at, last_visited_at, last_watched_at, recent_watched, viewed_but_not_started, started_but_not_finished, finished, last_email_notification, last_in_app_notification, last_sms_notification)
    VALUES");
    for user in users {
        sql.push_str(&format!(
            "('{}', '{}', '{}', '{}', '{}', {}::int[], {}::int[], {}::int[], {}::int[], '{}', '{}', '{}'),",
            user.email,
            user.name,
            user.created_at,
            user.last_visited_at,
            user.last_watched_at,
            list_to_string(user.recent_watched),
            list_to_string(user.viewed_but_not_started),
            list_to_string(user.started_but_not_finished),
            list_to_string(user.finished),
            user.last_email_notification,
            user.last_in_app_notification,
            user.last_sms_notification,
        ));
    }

    let v = &sql[..sql.len() - 1];
    sqlx::query(v).execute(pool).await?;

    Ok(())
}

fn list_to_string(list: Vec<i32>) -> String {
    format!("ARRAY{:?}", list)
}

#[allow(dead_code)]
async fn bulk_insert(users: HashSet<UserStat>, pool: &PgPool) -> Result<()> {
    let mut tx = pool.begin().await?;
    for user in users {
        let query = sqlx::query(
           r#"
            INSERT INTO user_stats(email, name, created_at, last_visited_at, last_watched_at, recent_watched, viewed_but_not_started, started_but_not_finished, finished, last_email_notification, last_in_app_notification, last_sms_notification)
            VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
           "#
        )
        .bind(&user.email)
        .bind(&user.name)
        .bind(user.created_at)
        .bind(user.last_visited_at)
        .bind(user.last_watched_at)
        .bind(&user.recent_watched)
        .bind(&user.viewed_but_not_started)
        .bind(&user.started_but_not_finished)
        .bind(&user.finished)
        .bind(user.last_email_notification)
        .bind(user.last_in_app_notification)
        .bind(user.last_sms_notification)
        ;
        tx.execute(query).await?;
    }
    tx.commit().await?;
    Ok(())
}

fn before(days: u64) -> DateTime<Utc> {
    Utc::now().checked_sub_days(Days::new(days)).unwrap()
}

fn now() -> DateTime<Utc> {
    Utc::now()
}

impl Hash for UserStat {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.email.hash(state);
    }
}

struct IntList(pub i32, pub i32, pub i32);

impl Dummy<IntList> for Vec<i32> {
    fn dummy_with_rng<R: Rng + ?Sized>(v: &IntList, rng: &mut R) -> Vec<i32> {
        let (max, start, len) = (v.0, v.1, v.2);
        let size = rng.gen_range(0..max);
        (0..size)
            .map(|_| rng.gen_range(start..start + len))
            .collect()
    }
}

struct UniqueEmail;
const ALPHABET: [char; 36] = [
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i',
    'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
];

impl Dummy<UniqueEmail> for String {
    fn dummy_with_rng<R: Rng + ?Sized>(_: &UniqueEmail, rng: &mut R) -> String {
        let email: String = SafeEmail().fake_with_rng(rng);
        let id = nanoid!(8, &ALPHABET);
        let at = email.find('@').unwrap();
        format!("{}.{}{}", &email[..at], id, &email[at..])
    }
}

impl Display for UserStat {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", serde_json::to_string(&self).unwrap())
    }
}


impl prost::Message for UserStat {
    impl prost::Message for UserStat {
        fn encode_length_delimiter(&self) -> Result<usize, prost::EncodeError> {
            prost::encoding::encode_length_delimiter(self.encoded_len())
        }
    
        fn encode(&self, buf: &mut Vec<u8>) -> Result<(), prost::EncodeError> {
            prost::encode_raw_bytes(buf, self.user_id.as_bytes());
            prost::encode_raw_varint32(buf, self.total_posts);
            prost::encode_raw_varint32(buf, self.total_likes);
            prost::encode_raw_varint32(buf, self.total_comments);
            Ok(())
        }
    
        fn merge_from(&mut self, src: &[u8]) -> Result<(), prost::DecodeError> {
            let mut decoder = prost::Decoder::new(src);
            while let Some(field_number) = decoder.next_tag()? {
                match field_number {
                    1 => self.user_id = decoder.read_string()?,
                    2 => self.total_posts = decoder.read_uint32()?,
                    3 => self.total_likes = decoder.read_uint32()?,
                    4 => self.total_comments = decoder.read_uint32()?,
                    _ => decoder.skip_field(field_number)?,
                }
            }
            Ok(())
        }
        fn encode_raw(&self, buf: &mut Vec<u8>) {
            self.email.encode_raw(buf);
            self.name.encode_raw(buf);
            self.gender.encode_raw(buf);
            self.created_at.encode_raw(buf);
            self.last_visited_at.encode_raw(buf);
            self.last_watched_at.encode_raw(buf);
            self.recent_watched.encode_raw(buf);
            self.viewed_but_not_started.encode_raw(buf);
            self.started_but_not_finished.encode_raw(buf);
            self.finished.encode_raw(buf);
            self.last_email_notification.encode_raw(buf);
            self.last_in_app_notification.encode_raw(buf);
            self.last_sms_notification.encode_raw(buf);
        }
    
        fn merge_from(&mut self, buf: &[u8]) -> Result<(), prost::DecodeError> {
            self.email.merge_from(buf)?;
            self.name.merge_from(buf)?;
            self.gender.merge_from(buf)?;
            self.created_at.merge_from(buf)?;
            self.last_visited_at.merge_from(buf)?;
            self.last_watched_at.merge_from(buf)?;
            self.recent_watched.merge_from(buf)?;
            self.viewed_but_not_started.merge_from(buf)?;
            self.started_but_not_finished.merge_from(buf)?;
            self.finished.merge_from(buf)?;
            self.last_email_notification.merge_from(buf)?;
            self.last_in_app_notification.merge_from(buf)?;
            self.last_sms_notification.merge_from(buf)?;
            Ok(())
        }
    
        fn merge_field(&mut self, field: u32, wire_type: prost::wire_type::WireType, buf: &[u8]) -> Result<(), prost::DecodeError> {
            match field {
                1 => self.email.merge_field(field, wire_type, buf)?,
                2 => self.name.merge_field(field, wire_type, buf)?,
                3 => self.gender.merge_field(field, wire_type, buf)?,
                4 => self.created_at.merge_field(field, wire_type, buf)?,
                5 => self.last_visited_at.merge_field(field, wire_type, buf)?,
                6 => self.last_watched_at.merge_field(field, wire_type, buf)?,
                7 => self.recent_watched.merge_field(field, wire_type, buf)?,
                8 => self.viewed_but_not_started.merge_field(field, wire_type, buf)?,
                9 => self.started_but_not_finished.merge_field(field, wire_type, buf)?,
                10 => self.finished.merge_field(field, wire_type, buf)?,
                11 => self.last_email_notification.merge_field(field, wire_type, buf)?,
                12 => self.last_in_app_notification.merge_field(field, wire_type, buf)?,
                13 => self.last_sms_notification.merge_field(field, wire_type, buf)?,
                _ => return Err(prost::DecodeError::new("unknown field")),
            }
            Ok(())
        }
        fn encoded_len(&self) -> usize {
            let mut len = 0;
            len += self.email.encoded_len();
            len += self.name.encoded_len();
            len += self.gender.encoded_len();
            len += self.created_at.encoded_len();
            len += self.last_visited_at.encoded_len();
            len += self.last_watched_at.encoded_len();
            len += self.recent_watched.encoded_len();
            len += self.viewed_but_not_started.encoded_len();
            len += self.started_but_not_finished.encoded_len();
            len += self.finished.encoded_len();
            len += self.last_email_notification.encoded_len();
            len += self.last_in_app_notification.encoded_len();
            len += self.last_sms_notification.encoded_len();
            len
        }
    
        fn clear(&mut self) {
            self.email.clear();
            self.name.clear();
            self.gender.clear();
            self.created_at.clear();
            self.last_visited_at.clear();
            self.last_watched_at.clear();
            self.recent_watched.clear();
            self.viewed_but_not_started.clear();
            self.started_but_not_finished.clear();
            self.finished.clear();
            self.last_email_notification.clear();
            self.last_in_app_notification.clear();
            self.last_sms_notification.clear();
        }
    }
    }


impl Default for UserStat {
    fn default() -> Self {
        Self {
            email: String::default(),
            name: String::default(),
            gender: gender::default(),
            created_at: DateTime::<Utc>::default(),
            last_visited_at: DateTime::<Utc>::default(),
            last_watched_at: DateTime::<Utc>::default(),
            recent_watched: Vec::default(),
            viewed_but_not_started: Vec::default(),
            started_but_not_finished: Vec::default(),
            finished: Vec::default(),
            last_email_notification: DateTime::<Utc>::default(),
            last_in_app_notification: DateTime::<Utc>::default(),
            last_sms_notification: DateTime::<Utc>::default(),
        }
    }
}

