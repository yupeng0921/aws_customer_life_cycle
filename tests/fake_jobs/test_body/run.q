begin {
}
body {
	$log("account_id method1: " + $1.account_id)
	$log("account_id method2: " + $account_id)
	$log("data count: " + $N)
	$log("ka: " + $1.ka)
	$$kb = "vb1"
	$log("kb: " + $$kb)
}
end {
}
