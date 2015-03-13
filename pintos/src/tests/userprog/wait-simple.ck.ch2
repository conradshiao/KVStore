# -*- perl -*-
use strict;
use warnings;
use tests::tests;
check_expected ([<<'EOF']);
((null)) begin
(child-simple) run
child-simple: exit(81)
((null)) wait(exec()) = 81
((null)) end
wait-simple: exit(0)
EOF
pass;
