#!/usr/bin/env python
# -*- encoding: utf-8 -*-
"""Pyunit for h2o.fillna"""
from __future__ import print_function
import sys
sys.path.insert(1,"../../")
import h2o
import pandas

def fillna():
    df = h2o.create_frame(rows=10000,
                          cols=3,
                          real_fraction=1.0,
                          real_range=100,
                          missing_fraction=0.3,
                          seed=123)
    # Pandas comparison
    pdf = df.as_data_frame()
    filledpdf = pdf.fillna(method="ffill",axis=0,limit=3)
    filledpdfh2o = h2o.H2OFrame(filledpdf)
    filled = df.fillna(method="forward",axis=0,maxlen=3)
    assert abs((filled - filledpdfh2o).sum(return_frame=False)) < 1e-11, "Difference between Pandas pivot too high"

if __name__ == "__main__":
    pyunit_utils.standalone_test(fillna)
else:
    fillna()