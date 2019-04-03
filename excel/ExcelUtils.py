#!/usr/bin/python
# -*- coding: utf-8 -*-
# -------------------------
# Author:   wangjj17
# Name:     ExcelUtils
# Date:     2019/4/3
# -------------------------
import xlsxwriter

context = ['0', 'LiMing', 'beijing', '18011112345', 'M', 18, 100]

def writeData(sheetSize, columnSize):
    # 创建工作簿
    wbk = xlsxwriter.Workbook('test.xlsx')
    for i in range(1, sheetSize):
        sheet_name = "sheet"+str(i)
        # 创建工作表
        sheet = wbk.add_worksheet(sheet_name)
        # 向单元格中写入数据
        for line in range(0, columnSize):
            for column in range(0, 7):
                if column == 0:
                    sheet.write(line, column, str(line))
                else:
                    sheet.write(line, column, context[column])
    # 关闭工作簿
    wbk.close()

if __name__ == "__main__":
    # 2007版之前Excel使用xlwt,后缀为xls,最大行65536，最多sheet255；
    # sheetSize = 255
    # columnSize = 65536
    # 2007版之后Excel使用xlsxwriter，后缀为xlsx，最大行1048576，最多无数个sheet；
    sheetSize = 255
    columnSize = 1048576
    writeData(sheetSize, columnSize)