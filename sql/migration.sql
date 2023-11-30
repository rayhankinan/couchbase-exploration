-- Query untuk migrasi ke Couchbase
with "A" as (
	select q.id, coalesce(
		json_agg(
			json_build_object(
				'id', a.id,
				'owneruserid', a.owneruserid,
				'parentid', a.parentid,
				'creationdate', a.creationdate,
				'score', a.score,
				'body', a.body
			)
		) filter (where a.id is not null),
		'[]'::json
	) as "answers"
	from questions q
	left join answers a on q.id = a.parentid
	group by q.id
), "B" as (
	select q.id, coalesce(json_agg(t.tag), '[]'::json) as "tags"
	from questions q
	left join tags t on t.id = q.id
	group by q.id
)
select to_json(
	json_build_object(
		'id', q.id,
		'owneruserid', q.owneruserid,
		'creationdate', q.creationdate,
		'score', q.score,
		'title', q.title,
		'body', q.body,
		'answers', "A"."answers",
		'tags', "B"."tags"
	)
) as "question"
from questions q
left join "A" on "A".id = q.id
left join "B" on "B".id = q.id;