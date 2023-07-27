#![deny(rust_2018_idioms)]

use chrono::{DateTime, Utc};
use iso8601_duration::Duration;
use itertools::{Either, Itertools};
use std::{collections::BTreeSet, ops};

type Timestamp = DateTime<Utc>;

pub trait ToTimestamp {
    fn to_timestamp(&self) -> Timestamp;
}

impl ToTimestamp for Timestamp {
    fn to_timestamp(&self) -> Timestamp {
        *self
    }
}

fn subtract_duration(now: Timestamp, duration: Duration) -> Timestamp {
    let duration = duration.to_chrono_at_datetime(now);
    now - duration
}

/// A marker type to indicate that data is sorted.
#[derive(Debug)]
struct Sorted<T>(pub T);

impl<T> Sorted<Vec<T>> {
    #[cfg(test)]
    fn unstable(mut data: Vec<T>) -> Self
    where
        T: Ord,
    {
        data.sort_unstable();
        Self(data)
    }

    fn unstable_by_key<K>(mut data: Vec<T>, f: impl FnMut(&T) -> K) -> Self
    where
        K: Ord,
    {
        data.sort_unstable_by_key(f);
        Self(data)
    }
}

impl<T> Sorted<T> {
    fn as_deref(&self) -> Sorted<&T::Target>
    where
        T: ops::Deref,
    {
        Sorted(self.0.deref())
    }

    fn into_inner(self) -> T {
        self.0
    }
}

impl<T> ops::Deref for Sorted<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug)]
pub struct Config(Vec<ConfigBin>);

impl Config {
    fn resolve(self, now: Timestamp) -> ResolvedConfig {
        let bins: Vec<_> = self.0.into_iter().map(|b| b.resolve(now)).collect();

        let bins = Sorted::unstable_by_key(bins, |b| b.older_than);

        ResolvedConfig { bins }
    }
}

#[derive(Debug)]
struct ConfigBin {
    older_than: Duration,
    keep_one_per: Duration,
}

impl ConfigBin {
    fn resolve(self, now: Timestamp) -> ResolvedConfigBin {
        let Self {
            older_than,
            keep_one_per,
        } = self;

        let older_than = subtract_duration(now, older_than);

        ResolvedConfigBin {
            older_than,
            keep_one_per,
        }
    }
}

#[derive(Debug)]
struct ResolvedConfig {
    bins: Sorted<Vec<ResolvedConfigBin>>,
}

#[derive(Debug)]
struct ResolvedConfigBin {
    older_than: Timestamp,
    keep_one_per: Duration,
}

impl ToTimestamp for ResolvedConfigBin {
    fn to_timestamp(&self) -> Timestamp {
        self.older_than
    }
}

#[derive(Debug, Copy, Clone)]
pub struct TimestampedItem<T> {
    pub timestamp: Timestamp,
    pub item: T,
}

impl<T> ToTimestamp for TimestampedItem<T> {
    fn to_timestamp(&self) -> Timestamp {
        self.timestamp
    }
}

#[derive(Debug)]
struct Cutoffs<'a, T>(&'a [T]);

impl<'a, T> Cutoffs<'a, T>
where
    T: ToTimestamp,
{
    fn new(cutoffs: Sorted<&'a [T]>) -> Self {
        Self(cutoffs.0)
    }

    fn bin_items<I>(&self, items: impl IntoIterator<Item = I>) -> Vec<Sorted<Vec<I>>>
    where
        I: ToTimestamp,
    {
        let mut bins: Vec<_> = std::iter::repeat_with(Vec::new)
            .take(self.0.len() + 1)
            .collect();

        for item in items {
            let (Ok(v) | Err(v)) = self
                .0
                .binary_search_by(|c| c.to_timestamp().cmp(&item.to_timestamp()));
            bins[v].push(item);
        }

        bins.into_iter()
            .map(|bin| Sorted::unstable_by_key(bin, |item| item.to_timestamp()))
            .collect()
    }
}

#[derive(Debug)]
pub struct Separated<I> {
    pub keep: Vec<I>,
    pub remove: Vec<I>,
}

impl<I> Default for Separated<I> {
    fn default() -> Self {
        Self {
            keep: Default::default(),
            remove: Default::default(),
        }
    }
}

impl<I> Separated<I> {
    fn keep_all(keep: Vec<I>) -> Self {
        Self {
            keep,
            remove: Vec::new(),
        }
    }

    fn concat(mut self, other: Self) -> Self {
        self.keep.extend(other.keep);
        self.remove.extend(other.remove);

        self
    }
}

fn one_bin<I>(keep_one_per: Duration, bin: Sorted<Vec<I>>) -> Separated<I>
where
    I: ToTimestamp,
{
    let (head, body) = match &bin[..] {
        [] | [_] | [_, _] => return Separated::keep_all(bin.into_inner()),
        [h, b @ .., _] => (h, b),
    };

    let mut prev_kept_item = head;
    let mut to_remove = BTreeSet::new();

    for (idx, candidate) in body.iter().enumerate() {
        // Account for skipping the head of the list
        let idx = idx + 1;

        let candidate_timestamp = candidate.to_timestamp();

        let age_since_last_kept_item = candidate_timestamp - prev_kept_item.to_timestamp();
        let keep_one_per = keep_one_per.to_chrono_at_datetime(candidate_timestamp);

        if age_since_last_kept_item < keep_one_per {
            to_remove.insert(idx);
        } else {
            prev_kept_item = candidate;
        }
    }

    let (keep, remove) = bin
        .into_inner()
        .into_iter()
        .enumerate()
        .partition_map(|(idx, item)| {
            if to_remove.contains(&idx) {
                Either::Right(item)
            } else {
                Either::Left(item)
            }
        });

    Separated { keep, remove }
}

pub fn everything<I>(config: Config, now: Timestamp, items: Vec<I>) -> Separated<I>
where
    I: ToTimestamp,
{
    let config = config.resolve(now);
    let cutoffs = Cutoffs::new(config.bins.as_deref());
    let bins = cutoffs.bin_items(items);

    config
        .bins
        .iter()
        .zip(bins)
        .map(|(bin_config, bin)| one_bin(bin_config.keep_one_per, bin))
        .reduce(Separated::concat)
        .unwrap_or_default()
}

#[cfg(test)]
mod test {
    use chrono::TimeZone;
    use proptest::{collection, prelude::*, sample::SizeRange, test_runner::TestCaseResult};

    use super::*;

    type Durations = Vec<Duration>;
    type SortedTimestamps = Sorted<Vec<Timestamp>>;

    fn sometime_in_2023() -> impl Strategy<Value = Timestamp> {
        const START_2023: i64 = 1672531200;
        const END_2023: i64 = 1703980800;

        (START_2023..=END_2023).prop_map(|d| Utc.timestamp_opt(d, 0).unwrap())
    }

    fn duration_up_to(up_to: Duration) -> impl Strategy<Value = Duration> {
        let Duration {
            year,
            month,
            day,
            hour,
            minute,
            second,
        } = up_to;

        let years = 0.0..=year;
        let months = 0.0..=month;
        let days = 0.0..=day;
        let hours = 0.0..=hour;
        let minutes = 0.0..=minute;
        let seconds = 0.0..=second;

        (years, months, days, hours, minutes, seconds).prop_map(
            |(year, month, day, hour, minute, second)| Duration {
                year,
                month,
                day,
                hour,
                minute,
                second,
            },
        )
    }

    fn basic_cutoff_duration() -> impl Strategy<Value = Duration> {
        duration_up_to(Duration {
            year: 2.0,
            month: 12.0,
            day: 31.0,
            hour: 24.0,
            minute: 60.0,
            second: 60.0,
        })
    }

    fn basic_keep_one_per_duration() -> impl Strategy<Value = Duration> {
        duration_up_to(Duration {
            year: 1.0,
            month: 1.0,
            day: 1.0,
            hour: 1.0,
            minute: 1.0,
            second: 1.0,
        })
    }

    fn basic_cutoff_durations() -> impl Strategy<Value = Durations> {
        collection::vec(basic_cutoff_duration(), 0..=10)
    }

    fn dates_in_recent_past() -> impl Strategy<Value = (Timestamp, SortedTimestamps)> {
        (sometime_in_2023(), basic_cutoff_durations()).prop_map(|(now, durations)| {
            let dates: Vec<_> = durations
                .into_iter()
                .map(|duration| subtract_duration(now, duration))
                .collect();
            let dates = Sorted::unstable(dates);
            (now, dates)
        })
    }

    proptest! {
        #[test]
        fn now_is_always_in_the_last_bin((now, cutoffs) in dates_in_recent_past()) {
            now_is_always_in_the_last_bin_impl(now, cutoffs)?;
        }
    }

    // What if the duration is 0?
    #[track_caller]
    fn now_is_always_in_the_last_bin_impl(
        now: Timestamp,
        cutoffs: SortedTimestamps,
    ) -> TestCaseResult {
        let cutoffs = Cutoffs::new(cutoffs.as_deref());
        let bins = cutoffs.bin_items([now]);

        let (tail, head) = bins.split_last().expect("Must always have 2 or more bins");
        for h in head {
            prop_assert!(h.is_empty());
        }
        prop_assert_eq!(tail.len(), 1);

        Ok(())
    }

    fn sorted_dates_in_2023(size: impl Into<SizeRange>) -> impl Strategy<Value = SortedTimestamps> {
        collection::vec(sometime_in_2023(), size).prop_map(Sorted::unstable)
    }

    proptest! {
        #[test]
        fn oldest_and_newest_in_bin_are_always_kept(
            keep_one_per in basic_keep_one_per_duration(),
            bin in sorted_dates_in_2023(1..=100),
        ) {
            oldest_and_newest_in_bin_are_always_kept_impl(keep_one_per, bin)?;
        }
    }

    #[track_caller]
    fn oldest_and_newest_in_bin_are_always_kept_impl(
        keep_one_per: Duration,
        bin: SortedTimestamps,
    ) -> TestCaseResult {
        let oldest = *bin.first().unwrap();
        let newest = *bin.last().unwrap();

        let Separated { keep, .. } = one_bin(keep_one_per, bin);

        prop_assert!(keep.contains(&oldest));
        prop_assert!(keep.contains(&newest));

        Ok(())
    }

    fn duration_with_timestamps_inside_duration(
        size: impl Into<SizeRange>,
    ) -> impl Strategy<Value = (Duration, SortedTimestamps)> {
        let duration = Duration {
            year: 1.0,
            month: 12.0,
            day: 31.0,
            hour: 24.0,
            minute: 60.0,
            second: 60.0,
        };

        (
            sometime_in_2023(),
            collection::vec(duration_up_to(duration), size),
            duration_up_to(duration),
        )
            .prop_map(|(now, mut durations, extra_duration)| {
                // Ensure there's always one duration
                durations.push(extra_duration);

                // Pair each duration with the resolved timestamp
                let mut duration_and_times: Vec<_> = durations
                    .into_iter()
                    .map(|d| (d, subtract_duration(now, d)))
                    .collect();

                // Find the oldest time ==> biggest duration
                duration_and_times.sort_unstable_by_key(|&(_, t)| t);
                let (duration, _) = duration_and_times.swap_remove(0);

                // Strip off the durations
                let times = duration_and_times
                    .into_iter()
                    .map(|(_, t)| t)
                    .chain([now])
                    .collect();

                (duration, Sorted::unstable(times))
            })
    }

    proptest! {
        #[test]
        fn only_oldest_and_newest_in_bin_are_kept(
            (keep_one_per, bin) in duration_with_timestamps_inside_duration(1..=100),
        ) {
            only_oldest_and_newest_in_bin_are_kept_impl(keep_one_per, bin)?;
        }
    }

    #[track_caller]
    fn only_oldest_and_newest_in_bin_are_kept_impl(
        keep_one_per: Duration,
        bin: SortedTimestamps,
    ) -> TestCaseResult {
        let oldest = *bin.first().unwrap();
        let newest = *bin.last().unwrap();

        let Separated { keep, .. } = one_bin(keep_one_per, bin);

        prop_assert_eq!(keep, [oldest, newest]);

        Ok(())
    }
}
