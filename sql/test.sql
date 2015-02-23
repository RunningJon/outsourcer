create or replace function fn_foo() returns text as
$$
declare
	v_return text;
begin
	select :myvar into v_return;
	return v_return;
end;
$$
language plpgsql;
