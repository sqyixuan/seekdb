
declare
begin
  for obj in (select job from user_jobs) loop
    begin
	  dbms_job.broken(obj.job, true, sysdate);
	  commit;
	  dbms_job.remove(obj.job);
	  commit;
	end;
  end loop;
end;
//



