begin {
}
body {
	if ($$bounce != 1 && $$complain != 1) {
		replacement = []
		$add(replacement, $1.account_id)
		if ($1.revenue1 > $1.revenue2)
			revenue = $1.revenue1
		else
			revenue = $1.revenue2
		if (revenue < 100) {
			if ($$Eng1Flag != 1 && $$Eng2Flag != 1) {
				if ($1.onboard > $get_current_time() - 3600 * 24 * 60)
					recent = 1
				else
					recent = 0
				if (revenue == 0 && recent == 1 && $$WelcomeFlag == 0) {
					$send_mail("conf.yaml", "welcome_subject.txt", "welcome_body.html", $1.email, replacement)
					$$WelcomeFlag == 1
					$log("welcome for: " + $1.account_id)
				} else {
					if ( $$Act1Flag == 0) {
						$send_mail("conf.yaml", "act1_subject.txt", "act1_body.html", $1.email, replacement)
						$$Act1Flag = 1
						$log("act1 for: " + $1.account_id)
					} else {
						if ( $$Act2Flag == 0) {
							$send_mail("conf.yaml", "act2_subject.txt", "act2_body.html", $1.email, replacement)
							$$Act2Flag = 1
							$log("act2 for: " + $1.account_id)
						}
					}
				}
			}
		} else {
			if ($$Eng1Flag == 0) {
				$send_mail("conf.yaml", "eng1_subject.txt", "eng1_body.html", $1.email, replacement)
				$$Eng1Flag = 1
				$log("eng1 for: " + $1.account_id)
			} else {
				if ($$Eng2Flag == 0) {
					$send_mail("conf.yaml", "eng1_subject.txt", "eng1_body.html", $1.email, replacement)
					$$Eng2Flag = 1
					$log("eng2 for: " + $1.account_id)
				}
			}
		}
	}
}
end {
}
