SELECT DISTINCT m.meet_name, m.meet_date_start, m.location
FROM meet_swimmer ms
JOIN swimmers s ON ms.swimmer_id = s.id
JOIN meets m ON ms.meet_id = m.id
WHERE s.first_name = 'First_name' AND s.last_name = 'Last_name'