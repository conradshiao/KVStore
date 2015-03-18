# -*- perl -*-
use strict;
use warnings;
use tests::tests;
check_expected ([<<'EOF']);
((null)) begin
((null)) begin
child-bad: exit(-1)
((null)) wait(exec()) = -1
((null)) end
wait-killed: exit(0)
EOF
pass;
