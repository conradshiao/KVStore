# -*- perl -*-
use strict;
use warnings;
use tests::tests;
check_expected ([<<'EOF']);
((null)) begin
(child-simple) run
child-simple: exit(81)
(child-simple) run
child-simple: exit(81)
(child-simple) run
child-simple: exit(81)
(child-simple) run
child-simple: exit(81)
((null)) end
exec-multiple: exit(0)
EOF
pass;
