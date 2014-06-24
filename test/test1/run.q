begin {
	account_list =  []
}

body {
	if ($N >= 2 && $1.revenue >= 10 && $1.revenue < 100 && $2.revenue < 10 && $$stage < 1) {
		replacement = []
		$add(replacement, $account_id)
		$send_mail("conf.yaml", "subject_stage1.txt", "body_stage1.html", $1.email, replacement)
		$add(account_list, $account_id)
		$$stage = 1
	}
}

end {
	count = 0
	$write_to_file("result.txt", "result:", "new")
	for account_id in account_list {
		$write_to_file("result.txt", account_id, "append")
		count = count + 1
	}
	$write_to_file("result.txt", count, "append")
	replacement = []
	$send_mail("conf.yaml", "subject_result.txt", "result.txt", "penyu@amazon.com", replacement)
}
