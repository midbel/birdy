create database if not exists planning;

use planning;

create table if not exists positions (
	id int not null auto_increment,
	name varchar(64) not null unique,
	primary key(id)
);

create table if not exists persons (
	id int not null auto_increment,
	firstname varchar(64) not null,
	lastname varchar(64) not null,
	abbr varchar(8) not null unique,
	allocated real not null default 1.0,
	active bool default true,
	position int not null,
	primary key(id),
	foreign key(position) references positions(id)
);

create table if not exists tags (
	id int not null auto_increment,
	name varchar(64) not null unique,
	primary key(id)
);

create table if not exists projects (
	id int not null auto_increment,
	name varchar(64) not null unique,
	code varchar(16) not null unique,
	summary varchar(512),
	dtstart date not null,
	dtend date not null,
	manager int not null,
	active bool not null default true,
	primary key(id),
	foreign key(manager) references persons(id),
	constraint projects_check_date_range check (dtstart < dtend)
);

create table if not exists projects_tags (
	project int not null,
	tag int not null,
	primary key(project, tag),
	foreign key(project) references projects(id),
	foreign key(tag) references tags(id)
);

create table if not exists sprints (
	id int not null auto_increment,
	name varchar(64) not null,
	summary varchar(512),
	dtstart date not null,
	dtend date not null,
	allocated real not null default 1.0,
	project int not null,
	parent int,
	primary key(id),
	foreign key (project) references projects(id),
	foreign key(parent) references sprints(id),
	constraint sprints_check_date_range check (dtstart < dtend),
	unique(name, project)
);

create table if not exists sprints_tags (
	sprint int not null,
	tag int not null,
	primary key(sprint, tag),
	foreign key(sprint) references sprints(id),
	foreign key(tag) references tags(id)
);

create table if not exists assignments (
	id int not null auto_increment,
	person int not null,
	sprint int not null,
	dtstart date null,
	dtend date null,
	manager bool default false,
	allocated real not null default 1.0,
	primary key(id),
	foreign key(person) references persons(id),
	foreign key(sprint) references sprints(id)
);

create table if not exists milestones (
	id int not null auto_increment,
	name varchar(64) not null,
	summary varchar(512),
	dom date not null,
	project int not null,
	primary key(id),
	foreign key(project) references projects(id),
	unique(name, project)
);

-- down
drop table if exists sprints_tags;
drop table if exists projects_tags;
drop table if exists assignments;
drop table if exists sprints;
drop table if exists milestones;
drop table if exists projects;
drop table if exists tags;
drop table if exists persons;
drop table if exists positions;