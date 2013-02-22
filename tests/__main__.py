#!/usr/bin/env python
import unittest

def main():
    import tests
    unittest.main(tests)

if __name__ == "__main__":
    import sys
    sys.exit(main(*sys.argv[1:]))
