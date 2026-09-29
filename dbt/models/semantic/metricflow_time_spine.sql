-- Time spine theo ngày cho MetricFlow: mỗi ngày một dòng. MetricFlow cần nó để
-- dựng trục thời gian (metric_time) khi truy vấn metric theo ngày/tháng.
select cast(d as date) as date_day
from unnest(sequence(date '2019-01-01', date '2030-12-31', interval '1' day)) as t(d)
