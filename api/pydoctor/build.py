#!/usr/bin/python
'''
Pydoctor API Runner
---------------------

Using pkg_resources, we attempt to see if pydoctor is installed,
if so, we use its cli program to compile the documents
'''
try:
    import sys, os
    import pkg_resources
    pkg_resources.require("pydoctor")

    from pydoctor.driver import main
    sys.argv = '''pydoctor.py --quiet
        --project-name=Mamba
        --project-url=http://github.com/bashwork/mamba/
        --add-package=../../mamba
        --html-output=html
        --html-write-function-pages --make-html'''.split()
    main(sys.argv[1:])
except: print "Pydoctor unavailable...not building"
