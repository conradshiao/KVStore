# -*- perl -*-
use strict;
use warnings;
use tests::tests;
check_expected ([<<'EOF']);
((null)) begin
(child-simple) run
child-simple: exit(81)
((null)) wait(exec()) = 81
((null)) wait(exec()) = -1
((null)) end
wait-twice: exit(0)
EOF
pass;
