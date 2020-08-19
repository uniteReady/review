create table job_infos(
id int primary key auto_increment,
task_name varchar(200),
totals int ,
formats int,
`errors` int,
run_times int ,
`day` varchar(200),
start_time varchar(50),
end_time varchar(50),
create_time timestamp not null default current_timestamp comment '创建时间',
update_time timestamp not null default current_timestamp on update current_timestamp comment '更新时间'
)ENGINE=INNODB charset utf8mb4;