#!/usr/bin/expect --

set src [lindex $argv 0]
set dest [lindex $argv 1]

set timeout 3600
#log_user 0
# spawn -noecho

spawn -noecho bash -c "rsync -vrtzp --exclude history --progress $src $dest"
expect {
	"*password*" { send "123456\r" }	
}
expect {
    eof { exit }
}