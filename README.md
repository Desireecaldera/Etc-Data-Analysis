# ETC_data_analysis

Working repository for data analysis and statistics for ETC data

This is a holding space for the initial analysis scripts used during summer SURF 2021

# CSV Generator

    `droid.py`
	 Droid Wrapper for python
	 Authored by Ethan Wolfe

    Usage:
		droid.py <T|S|P>`<Working Dir>` `<Output Dir>` `<Blacklisted Names>`
		droid.py `<R>` `<Output Dir>`

    Level:
		The level of the working_dir that you are giving it.
		T (Top-Level) - e.g.`D:/`
		S (Semester-Level) - e.g. `D:/2017_semester_1`
		P (Project-Level) - e.g. `D:/2017_semester_1/wfk`
		R (Restart Crashed)

    Working Dir:
		The directory that you want the program to search and call droid on

    Output Dir:
		The output directory that you want the program to generate output folders into

    Blacklisted Names:
		This is list of names that you don't want to be scanned.
		For a project level scan, other than the project name, there is nothing to blacklist.
		For a Semester level scan, blacklsit names would be project folder names
		For a Top level scan, a blacklist name could either be a semester folder name, a project folder name or a combination of the two. e.g. 2018_semester_2 or 2018_semester_2/arthmagic

    Requires droid to be installed into the default directory.

    It is advised that you clear out your ~/.droid file of profiles to avoid
	 conflicts caused by old droid profiles that might have the same profile id after generation. This will also save you some much needed disk space
