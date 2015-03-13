# -*- perl -*-
use strict;
use warnings;
use tests::tests;
check_expected ([<<'EOF', <<'EOF', <<'EOF', <<'EOF']);
((null)) begin
load: no-such-file: open failed
((null)) exec("no-such-file"): -1
((null)) end
exec-missing: exit(0)
EOF
((null)) begin
((null)) exec("no-such-file"): -1
((null)) end
exec-missing: exit(0)
EOF
((null)) begin
load: no-such-file: open failed
no-such-file: exit(-1)
((null)) exec("no-such-file"): -1
((null)) end
exec-missing: exit(0)
EOF
((null)) begin
load: no-such-file: open failed
((null)) exec("no-such-file"): -1
no-such-file: exit(-1)
((null)) end
exec-missing: exit(0)
EOF
pass;
