# !/usr/bin/env python
# coding: utf-8

import project.allitebooks.result
import project.verycd.china_result
import project.verycd.occident_result
import project.verycd.classic_result

if __name__ == '__main__':
    # project.allitebooks.result.build_db()
    # project.allitebooks.result.run()

    # project.verycd.result.build_db()
    # project.verycd.china_result.run()
    # project.verycd.occident_result.build_db()
    # project.verycd.occident_result.run()

    project.verycd.classic_result.build_db()
    project.verycd.classic_result.run()