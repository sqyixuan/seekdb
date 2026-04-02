delimiter //;

  CREATE OR REPLACE PACKAGE "OPS_UTILS" is
	
	type cursorType is ref cursor;

	
	procedure tokenizer(iStart in number, sPattern in varchar2, sBuffer in varchar2, sResult out varchar2, sNextBuffer out varchar2);

	
	procedure foundIn(src in varchar2, str in varchar2, founded out boolean);

	
	procedure append(src in varchar2, str in varchar2, results out varchar2);

	
	procedure remove(src in varchar2, str in varchar2, results out varchar2);

	
	procedure stringToArray1(src in varchar2, results out NT, length out number);

	
	procedure stringToArray2(src	in varchar2, results out	VT, length out number);

	
	procedure stringToArray3(src in varchar2, pattern in varchar2, results out NT, length out number);

	
	procedure stringToArray4(src	in varchar2, pattern in varchar2, results out VT, length out number);

end OPS_UTILS;


//

  CREATE OR REPLACE PACKAGE BODY "OPS_UTILS" is
	
	procedure tokenizer(iStart in number, sPattern in varchar2, sBuffer in varchar2, sResult out varchar2, sNextBuffer out varchar2)
	is
		nPos1 number;
    begin
		nPos1 := instr(sBuffer, sPattern, iStart);
		dbms_output.put_line(nPos1);
		if (nPos1 = 0 and sBuffer is not null) then
			sResult := sBuffer;
			sNextBuffer := null;
			dbms_output.put_line(sResult);
		else
			sResult := substr(sBuffer, 1, nPos1-1);
			sNextBuffer := substr(sBuffer, nPos1+1);
			dbms_output.put_line(sResult);
			dbms_output.put_line(sNextBuffer);
		end if;		
		exception when others then
			dbms_output.put_line('ERROR: an error occurred in procedure tokenizer:');
	end tokenizer;

	
	procedure foundIn(src in varchar2, str in varchar2, founded out boolean)
	is
		temp_src varchar2(32767) := '';
		temp_str varchar2(32767) := '';
		temp_nextstr varchar2(32767) := ' ';
	begin
		if src is null or str is null then
			founded := false;
		else
			founded := false;
			temp_src := src;
			while(temp_nextstr is not null) loop
				tokenizer(1, ',', temp_src, temp_str, temp_nextstr);
				if temp_str = str then
					founded := true;
					return;
				end if;
				temp_src := temp_nextstr;
			end loop;
		end if;

		
		exception when others then
			dbms_output.put_line('ERROR: an error occurred in procedure foundin:');
	end foundin;

	
	procedure append(src in varchar2, str in varchar2, results out varchar2)
	is
		temp_founded boolean := false;
	begin
		if src is null then
			if str is null then
				results := '';
			else
				results := str;
			end if;
		else
			if str is null then
				results := src;
			else
				foundin(src, str, temp_founded);
				if temp_founded then
					results := src;
				else
					results := src || ',' || str;
				end if;
			end if;
		end if;

		
		exception when others then
			dbms_output.put_line('ERROR: an error occurred in procedure append:');
	end append;

	
	procedure remove(src in varchar2, str in varchar2, results out varchar2)
	is
		temp_src varchar2(32767) := '';
		temp_str varchar2(32767) := '';
		temp_nextstr varchar2(32767) := ' ';
	begin
		if src is null or str is null then
			results := '';
		else
			temp_src := src;
			while(temp_nextstr is not null) loop
				tokenizer(1, ',', temp_src, temp_str, temp_nextstr);
				if temp_str != str then
					results := results || temp_str || ',';
				end if;
				temp_src := temp_nextstr;
			end loop;
			results := substr(results, 0, length(results)-1);
		end if;

		
		exception when others then
			dbms_output.put_line('ERROR: an error occurred in procedure remove:');
	end remove;

	
	procedure stringToArray1(src in varchar2, results out NT, length out number)
	is
	begin
		stringToArray3(src, ',', results, length);
	end stringToArray1;

	
	procedure stringToArray2(src	in varchar2, results out VT, length out number)
	is
	begin
		stringToArray4(src, ',', results, length);
	end stringToArray2;

	
	procedure stringToArray3(src in varchar2, pattern in varchar2, results out NT, length out number)
	is
		temp_src varchar2(32767) := src;
		temp_str varchar2(32767) := null;
		temp_next varchar2(32767) := ' ';
		temp_nt NT :=  NT();
		i number := 0;
	begin
		while (temp_next is not null) loop
			tokenizer(1, pattern, temp_src, temp_str, temp_next);
			i := i+1;
			length := i;
			temp_nt.extend();
			temp_nt(temp_nt.count) := temp_str;
			temp_src := temp_next;
		end loop;
		results := temp_nt;

		
		exception when others then
			dbms_output.put_line('ERROR: an error occurred in procedure stringtoarray:');
	end stringToArray3;

	
	procedure stringToArray4(src	in varchar2, pattern in varchar2, results out	VT, length out number)
	is
		temp_src varchar2(32767) := src;
		temp_str varchar2(32767) := null;
		temp_next varchar2(32767) := ' ';
		temp_vt VT :=  VT();
		i number := 0;
	begin
		while (temp_next is not null) loop
			tokenizer(1, pattern, temp_src, temp_str, temp_next);
			i := i+1;
			length := i;
			temp_vt.extend();
			temp_vt(temp_vt.count) := temp_str;
			temp_src := temp_next;
		end loop;
		results := temp_vt;

		
		exception when others then
			dbms_output.put_line('ERROR: an error occurred in procedure stringtoarray:');
	end stringToArray4;

end OPS_UTILS;

//



