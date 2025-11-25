SELECT meets.meet_name, meets.meet_date_start, swimmers.first_name, swimmers.last_name, swimmers.gender, swimmers.birth_date, swimmers.mm_number, teams.team_name, teams.team_type 
FROM swimmers
JOIN meet_team_swimmer ON swimmers.id = meet_team_swimmer.swimmer_id
JOIN meets ON meets.id = meet_team_swimmer.meet_id
JOIN teams ON teams.id = meet_team_swimmer.team_id
WHERE swimmers.last_name <> ""
ORDER BY meets.meet_year DESC, meets.meet_name, swimmers.last_name, swimmers.first_name;