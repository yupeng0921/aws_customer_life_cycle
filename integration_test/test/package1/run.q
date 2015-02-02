begin {
	$log("test_start")
	a = 1 + 1
	$log("test_plus=" + a)
	a = 2 * 3
	$log("test_multiple=" + a)
}
body {
	prev = $get_item_by_date($1.date, "before", "1", "day")
	if (prev) {
		if ($N >= 2 && $1.revenue >= 10 && $1.revenue < 100 && prev.revenue < 10 && $$stage < 1) {
			replacement = []
			$add(replacement, $account_id)
			$send_mail("conf.yaml", "subject.txt", "body.html", $1.email, replacement)
		}
	} else {
		$log("no previous: " + $account_id)
	}
}
end {
	$log("test_done")
}