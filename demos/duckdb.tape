Output demos/duckdb.gif

Set FontSize 24
Set Width 2400
Set Height 1200
Set Framerate 60

Type@150ms "make duckdb"
Enter
Wait+Line /D/
Sleep 1

Type@150ms "select * from bronze.events limit 10;"
Enter
Wait+Line /D/
Sleep 10

Type@150ms "select * from dead_letters.events limit 10;"
Enter
Wait+Line /D/
Sleep 10

Type@150ms "select count(*) from bronze.events;"
Enter
Wait+Line /D/
Sleep 10

Type@150ms "select count(*) from dead_letters.events;"
Enter
Wait+Line /D/
Sleep 10

Type@150ms "select _dead_letter_reason, count(*) from dead_letters.events group by _dead_letter_reason;"
Enter
Wait+Line /D/
Sleep 10
