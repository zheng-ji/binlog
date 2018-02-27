from mm_envs import *


cc_library(
	target_bases = ['mmcomm',],
	name = 'binlog',
	srcs = [
		'binlog.cpp',
	],
	deps = [
        '//comm2/core:core',
        '//mmux/mmuxexpstatbf/bloom_filter:bloom_filter'
	],
    export_incs = [
    	'.',
    ],
    extra_cppflags = [
        '-Wall',
        '-Werror',
    ]    

)

cc_binary(
	target_bases = ['mmcomm',],
	name = 'testbinlog',
	srcs = [
        'testbinlog.cpp',
	],
	deps = [
        ':binlog',
	],
    export_incs = [
    	'.',
    ],
    extra_cppflags = [
        '-Wall',
        '-Werror',
        '-std=c++11',
    ]    

)

cc_binary(
	target_bases = ['mmcomm',],
	name = 'binlog_performance_test',
	srcs = [
        'binlog_performance_test.cpp',
	],
	deps = [
        ':binlog',
        '//comm2/core:core',
	],
    export_incs = [
    	'.',
    ],
    extra_cppflags = [
        '-Wall',
        '-Werror',
        '-std=c++11',
    ]    

)
