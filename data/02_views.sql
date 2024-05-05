create view vassignments as (
	select
	a.id,
	s.id sprint,
	t.id project,
	p.id person,
	p.firstname,
	p.lastname,
	j.name position,
	a.allocated,
	ifnull(a.dtstart, s.dtstart) dtstart,
	ifnull(a.dtend, s.dtend) dtend,
	a.dtstart = s.dtstart and a.dtend = s.dtend complete
	from assignments a 
		join sprints s on a.sprint=s.id 
		join persons p on a.person = p.id 
		join positions j on p.position = j.id 
		join projects t on t.id=s.project
);

-- down
drop view if exists vassignments;