# -*- perl -*-
use strict;
use warnings;
use tests::tests;
check_expected ([<<'EOF', <<'EOF']);
((null)) begin
((null)) end
wait-bad-pid: exit(0)
EOF
((null)) begin
wait-bad-pid: exit(-1)
EOF
pass;
