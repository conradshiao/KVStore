# -*- perl -*-
use strict;
use warnings;
use tests::tests;
check_expected ([<<'EOF', <<'EOF']);
((null)) begin
((null)) end
exec-bad-ptr: exit(0)
EOF
((null)) begin
exec-bad-ptr: exit(-1)
EOF
pass;
