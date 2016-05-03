#!/usr/bin/env python

import pandas as pd


def get_depth_data(df):
    refdes_column = [c for c in df.columns if 'ooi' in c.lower()]
    depth_column = [c for c in df.columns if 'depth' in c.lower()]
    if len(refdes_column) == 1 and len(depth_column) == 1:
        df = df[refdes_column + depth_column]
        df = df.dropna()
        return df.rename(index=str, columns={refdes_column[0]: 'designator', depth_column[0]: 'depth'})


def parse_file(filepath, outpath):
    frames = []
    sheets = pd.read_excel(filepath, sheetname=None)
    for sheet in sheets:
        df = get_depth_data(sheets[sheet])
        if df is not None:
            frames.append(df)
    df = pd.concat(frames)
    df = df.sort_values('designator')
    df.to_csv(outpath, index=False)


def main():
    import sys
    parse_file(*sys.argv[1:3])


if __name__ == '__main__':
    main()