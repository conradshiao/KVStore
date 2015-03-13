# -*- perl -*-
use strict;
use warnings;
use tests::tests;
check_expected ([<<'EOF']);
((null)) begin
(child-simple) run
child-simple: exit(81)
((null)) end
exec-once: exit(0)
EOF
pass;
