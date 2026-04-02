##ADD_DATA##
drop procedure if exists insert_data;
delimiter //;
create PROCEDURE insert_data(tab_nam varchar(100),var int)
begin
	set @i = 1;
	
	while @i <= var do
		SET @IN_VA_SQL = CONCAT(
		"INSERT INTO ",tab_nam," VALUES (",@i,",",@i,");");
		
		PREPARE STMT FROM @IN_VA_SQL;
		EXECUTE STMT;
		
		set @i = @i+1;
	end while;
	deallocate PREPARE STMT;
end;
//
delimiter ;//

drop procedure if exists insert_data_oneval;
delimiter //;
create PROCEDURE insert_data_oneval(tab_nam varchar(20),var int)
begin
	set @i = 1;
	SET @IN_VA_SQL = CONCAT("INSERT INTO ",tab_nam," VALUES (1,1);");
	PREPARE STMT FROM @IN_VA_SQL;
	while @i <= var do
		EXECUTE STMT;
		set @i = @i+1;
	end while;
	deallocate PREPARE STMT;
end;
//
delimiter ;//

drop procedure if exists insert_data_onecol;
delimiter //;
create PROCEDURE insert_data_onecol(tab_nam varchar(20),col varchar(20),var int)
begin
	set @i = 1;
	SET @IN_VA_SQL = CONCAT("INSERT INTO ",tab_nam,"(",col,") VALUES (1);");
	PREPARE STMT FROM @IN_VA_SQL;
	while @i <= var do
		EXECUTE STMT;
		set @i = @i+1;
	end while;
	deallocate PREPARE STMT;
end;
//
delimiter ;//
