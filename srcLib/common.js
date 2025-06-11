const { log: L } = console
const SheetUtils = {
  safeRemoveFilter: function (ss) {
    try {
      const filter = ss.getFilter();
      if (filter) filter.remove();
    } catch (e) { /* ignore */ }
  },
  insertRowsForNewData: function (ss, lastRow, dataLength) {
    //1. опредлить нужно ли добавлять строки для записи
    let maxRows = ss.getMaxRows()
    let howMany = lastRow + dataLength - maxRows
    if (howMany > 0) { ss.insertRowsAfter(maxRows, howMany); console.log(`Добавлено пустых строк:${howMany}`) }
  },
  fillLastUndefined: function (table, length_row = 15, filler = "") {
    table.forEach((row) => {
      for (i = length_row; i >= 0; i--) {
        if (row[i] !== undefined) { break }
        row[i] = filler;
      }
    });
  },
  clearByAddBlankRow: function (table, addRows) {
    let maxRowLength = table.reduce((maxLength, row) => {
      if (row.length > maxLength) maxLength = row.length;
      return maxLength;
    }, 0);
    const blankArr = Array(addRows).fill(Array(maxRowLength).fill(""));
    table = table.concat(blankArr);
    return table;
  },
  // *******Группировка таблиц**********************************
  group_list: function (rows_arr, clmns_group_by, clmns_to_sum) {
    // L('группировка oldArr: ')
    // L(rows_arr.slice(0,10))
    rows_arr.forEach((e, i) => {
      // добавляет в массив колонку(в конец) с значением групировки объединенным из разных столбцов исходного массива
      // нужно для групппировки по нескольким полям 

      let group_value = ''
      clmns_group_by.forEach(function (k) { group_value += rows_arr[i][k] })
      rows_arr[i].push(group_value)
    })
    let i1 = rows_arr[0].length
    let i = i1 - 1

    let group_to_values = rows_arr.reduce(function (obj, item) {
      obj[item[i]] = obj[item[i]] || [];
      obj[item[i]].push(item);
      return obj;
    }, {});

    let valuesArr = Object.values(group_to_values)
    let newArr = []
    valuesArr.forEach(function (elm) {
      let row = SheetUtils.colSum(elm, clmns_to_sum)
      row.pop()
      newArr.push(row)
    })
    // L('newArr: ')
    // L(newArr.slice(0,10))
    return newArr
  },
  groupListFast: function (rows_arr, clmns_group_by, clmns_to_sum) {
    const groupMap = {};
    const rowsCount = rows_arr.length;

    for (let i = 0; i < rowsCount; i++) {
      const row = rows_arr[i];
      let groupKey = clmns_group_by.map(k => row[k]).join('|');

      if (groupKey in groupMap) {
        const group = groupMap[groupKey];
        clmns_to_sum.forEach(col => {
          group.sums[col] = (group.sums[col] || 0) + (row[col] || 0);
        });
      } else {
        groupMap[groupKey] = {
          groupFields: clmns_group_by.map(k => row[k]),
          sums: clmns_to_sum.reduce((acc, col) => {
            acc[col] = row[col] || 0;
            return acc;
          }, {})
        };
      }
    }

    return Object.values(groupMap).map(group => {
      const resultRow = [...group.groupFields];
      clmns_to_sum.forEach(col => {
        resultRow[col] = group.sums[col];
      });
      return resultRow;
    });
  },
  // *******Блок обновления таблиц *********************************
  updateTable: function (old_table, new_table, key_fields_nums) {
    // принимает аргументами две таблицы, удаляет по ключевым полям значения из старой таблицы замещая строками из новой таблицы
    // если в новой таблице нет ключей соответ старым ключам, оставляет старые записи, добавляет новые строки.
    //!!! В каждой таблице строки должны быть уникальны в пределах таблицы, иначе дубликаты затрутся

    let old_t_obj = SheetUtils.arr_to_obj_key(old_table, key_fields_nums);
    let new_t_obj = SheetUtils.arr_to_obj_key(new_table, key_fields_nums);
    Object.assign(old_t_obj, new_t_obj)
    let new_tab = Object.values(old_t_obj)
    return new_tab
  },
  update_table_keep_column: function (old_table, new_table, key_fields_nums, keep_col) {
    // принимает аргументами две таблицы, замещает по ключам значения из страой таблицы новыми
    // если в новой таблице нет ключей соответ старым ключам, оставляет старые записи, добавляет/переписывает новые если есть.
    // сохраняет значение из колонки keep_col в обновленных данных: update_table_keep_column(old_table, new_table, [0,1], 3)
    let old_t_obj = SheetUtils.arr_to_obj_key(old_table, key_fields_nums);
    let new_t_obj = SheetUtils.arr_to_obj_key(new_table, key_fields_nums);

    for (row in new_t_obj) {
      old_t_obj[row] = new_t_obj[row].concat(old_t_obj[row] ? old_t_obj[row][keep_col] : "")
    }
    return Object.values(old_t_obj)
  },
  updateTableKeepColumns: function (old_table, new_table, key_fields_nums, keep_cols) {
    let old_t_obj = SheetUtils.arr_to_obj_key(old_table, key_fields_nums);
    let new_t_obj = SheetUtils.arr_to_obj_key(new_table, key_fields_nums);
    let new_obj = {};
    Object.assign(new_obj, old_t_obj, new_t_obj);
    for (let row in old_t_obj) {
      keep_cols.forEach(function (col) {
        new_obj[row][col] = old_t_obj[row][col];
      });
    }
    return Object.values(new_obj);
  },
  arr_to_obj_key: function (arr, key_fields_nums) {
    // обращает таблицу в объект, назначая ключом для каждой строки сочетание значений столбцов, указанных в key_fields_nums
    // key_fields_nums = [0,] - массив индексов колонок
    // arr = [['a', 1], ['b', 2], ['c', 3]] - таблица значений;
    // на выходе: {"a":[['a', 1],"b":['b', 2],"c":['c', 3]]}

    const res = arr.reduce(function (acc, curr) {
      let key_field = ''
      for (let k of key_fields_nums) {
        key_field += curr[k]
      }
      acc[key_field] = curr
      return acc
    }, {});
    return res
  },
  getDifferenceTab: function (new_tab, old_tab) {
    // возвращает таблицу из строк new_tab, которых нет в old_tab
    let old_tab_keys = old_tab.map((r) => {
      let t = [...r]
      for (i = 8; i < 18; i += 1) { t[i] = (parseInt(r[i])).toLocaleString("ru-RU") }
      return t.toString()
    })
    let nw_tab = []
    new_tab.forEach(function (r) {
      let t = [...r]
      for (i = 8; i < 18; i += 1) { t[i] = (parseInt(r[i])).toLocaleString("ru-RU") }
      L("элемент: ", t.toString(), " найден в строке #: ", old_tab_keys.indexOf(t.toString()))
      if (old_tab_keys.indexOf(t.toString()) === -1) { nw_tab.push(r) }
    })
    return nw_tab
  },
  tofillSkipCells: function (table, las_elm_ind, filler = "") {
    // выравнивает длинну строк таблицы table до величины row_length, заполняя последний элемент более коротких строк
    //  значением из fill. Промежуточные недостающие элементы оставляет пустыми (Nan). Нужно для записи через API.readRangeSS->writeToSS
    table.forEach((elm) => { if (elm[las_elm_ind] === undefined) { elm[las_elm_ind] = filler } })
    return table
  },
  fillAllUndefined: function (table, length_row = 15, filler = "") {
    // Заполняет все пустые(undefined) значения таблицы table filler-ом на длину строки length_row
    // const runTimer = Date.now()
    table.forEach((row) => {
      for (i = 0; i < length_row; i++) {
        if (row[i] === undefined) {
          row[i] = filler
        }
      }
    })
    // console.log("runtime is:", Date.now() - runTimer)
    return table
  },
  colSum: function (arr, clmns_to_sum, to_round = false) {
    // сворачивает колонки массива arr суммируя перечисленные в списке[clmns_to_sum] 
    // !!! остальные значения колонок заменяет значениями из первой строки
    let out = arr[0]
    // arr[0].forEach(function (e) { out.push(e) })
    for (let i = 1; i < arr.length; i++) {
      for (let j of clmns_to_sum) {
        if (to_round) {
          out[j] = Math.round((+ out[j] + (arr[i][j] || 0)) * 100) / 100
        } else {
          out[j] = + out[j] + (arr[i][j] || 0)
        };
      }
    }
    return out;
  }
};
const ApiUtils = {
  batchWriteToSS: function (spreadsheetId, values, ranges, valueInputOption = "USER_ENTERED") {
    try {
      const valueRange = ranges.map((range, ind) => { return { "range": range, "values": values[ind] } })
      const resource = { data: valueRange, valueInputOption: valueInputOption };
      Sheets.Spreadsheets.Values.batchUpdate(resource, spreadsheetId)
      console.info(`batchWriteToSS ok!`);
      return true;
    } catch (e) {
      console.error(`batchWriteToSS failed with error ${e.stack}`);
      throw new Error(e.stack);
    }
  },
  writeToSS: function (spreadsheetId, rowValues, range, valueInputOption = 'USER_ENTERED', okMsg = "writeToSS") {
    // let range = 'Лист1!B1'
    const request = {
      'valueInputOption': valueInputOption,
      'data': [
        {
          'range': range,
          'majorDimension': 'ROWS',
          'values': rowValues
        }
      ]
    };
    try {
      const response = Sheets.Spreadsheets.Values.batchUpdate(request, spreadsheetId);
      if (response) {
        console.log(`${okMsg}: ok!`);
        return true;
      }
      console.log('response null');
      return false
    } catch (e) {
      console.log(`${okMsg}: failed with error ${e.message}`);
      return false
    }
  },
  appendToSS: function (spreadsheetId, rowValues, range, valueInputOption = "USER_ENTERED", okMsg = "appendToSS") {
    let resource = {
      majorDimension: "ROWS",
      values: rowValues,
    };
    let optionalArgs = { valueInputOption: valueInputOption };
    try {
      const response = Sheets.Spreadsheets.Values.append(
        resource,
        spreadsheetId,
        range,
        optionalArgs
      );
      if (response) {
        console.log(`${okMsg}: ok!`);
        return true;
      }
      console.log("response null");
      return false;
    } catch (e) {
      console.log("appendToSS failed with error %s", e.message);
      throw new Error(`${okMsg}: ошибка записи данных ${e.message}`);
    }
  },
  readRangeSS: function (spreadsheetId, range, valueRenderOption = "UNFORMATTED_VALUE", dateTimeRenderOption = "FORMATTED_STRING") {
    try {
      optionalArgs = {
        dateTimeRenderOption: dateTimeRenderOption,// FORMATTED_STRING || SERIAL_NUMBER
        valueRenderOption: valueRenderOption  // UNFORMATTED_VALUE || FORMATTED_VALUE || FORMULA
      }
      const response = Sheets.Spreadsheets.Values.get(
        spreadsheetId, range, optionalArgs
      );
      if (response.values) {
        return response.values;
      }
      console.info(`(Не ошибка) readRangeSS. Попытка чтения пустого диапазона: ${range}. return []`);
      return []
    } catch (e) {
      console.log('readRangeSS failed with error %s', e.message);
      return false
    }
  },
  batchUpdateTextFormatSS: function (SS, ss, range) {
    // SS, ss: as spreasheet and sheet objects, range: as "A1:D4"
    const spreadsheetId = SS.getId()
    const SHEET_ID = ss.getSheetId()
    const RC = ApiUtils.toStartEndIndexes(ss, range)
    try {
      let resource = {
        "requests": [
          {
            "repeatCell": {
              "range": {
                "sheetId": SHEET_ID,
                "startRowIndex": RC.startRowIndex,
                "endRowIndex": RC.endRowIndex,
                "startColumnIndex": RC.startColumnIndex,
                "endColumnIndex": RC.endColumnIndex
              },
              "cell": {
                "userEnteredFormat": {
                  "numberFormat": {
                    "type": "TEXT"
                    // "type": "NUMBER",
                    // "pattern": "@"
                  }
                }
              },
              "fields": "userEnteredFormat.numberFormat"
            }
          }
        ]
      }
      const response = Sheets.Spreadsheets.batchUpdate(resource, spreadsheetId)
      if (response) {
        return response;
      }
      console.info(`формат для ${range} не установлен`);
      return []
    } catch (e) {
      console.log('batchUpdateSS failed with error %s', e.message);
      return []
    }
  },
  batchReadRangeSS: function (spreadsheetId, ranges, valueRenderOption = "UNFORMATTED_VALUE", dateTimeRenderOption = "FORMATTED_STRING", majorDimension = "ROWS") {
    /* let ranges = ['Продажи позаказно!B1:C20','Продажи позаказно!E5:F200'] */
    try {
      let optionalArgs = {
        dateTimeRenderOption: dateTimeRenderOption, // FORMATTED_STRING || SERIAL_NUMBER
        valueRenderOption: valueRenderOption, // UNFORMATTED_VALUE || FORMATTED_VALUE || FORMULA
        majorDimension: majorDimension, //"ROWS" || "COLUMNS"
        ranges: ranges,
      };
      const response = Sheets.Spreadsheets.Values.batchGet(spreadsheetId, optionalArgs);
      if (response.valueRanges) {
        return response.valueRanges;
      }
      console.info(
        `(Не ошибка) batchReadRangeSS.Попытка чтения пустого диапазона: ${ranges}.return[]`
      );
      return [];
    } catch (e) {
      console.log("batchReadRangeSS failed with error %s", e.message);
      return [];
    }
  },
  getAllRangesValues: function (SS, ranges, valueRenderOption = "UNFORMATTED_VALUE", dateTimeRenderOption = "FORMATTED_STRING", majorDimension = "ROWS") {
    let dataRanges = this.batchReadRangeSS(SS.getId(), ranges, valueRenderOption, dateTimeRenderOption, majorDimension);
    let rangesValues = dataRanges.map((range) => range.values || []);
    return rangesValues;
  },
  toStartEndIndexes: function (ss, reference) {
    const range = ss.getRange(reference);
    const r1c1 = { startRowIndex: range.getRow() - 1, startColumnIndex: range.getColumn() - 1 }
    r1c1.endRowIndex = range.getNumRows() + r1c1.startRowIndex
    r1c1.endColumnIndex = range.getNumColumns() + r1c1.startColumnIndex
    return r1c1;
  },
};
const DateUtils = {
  getSerialNumberDateNoGMT: function (date) { return (date.getTime()) / 864e5 + 25569; },
  getSerialNumberDate: function (date) { return (date.getTime() + 3 * 60 * 60 * 1000) / 864e5 + 25569; },
  getDateFromSerialNumberDate: function (dateSer) { return new Date((dateSer - 25569) * 864e5 - 3 * 60 * 60 * 1000) },
  getStrDateFromSerialNumberDate: function (dateSer) { return Utilities.formatDate(new Date((dateSer - 25569) * 864e5), "GMT", "yyyy-MM-dd") },
  getStrDateTimeFromSerialNumberDate: function (dateSer) { return Utilities.formatDate(new Date((dateSer - 25569) * 864e5), "GMT", "yyyy-MM-dd HH:mm:ss") },
  /*covert "01.01.2020" or "01-01-2020" to Date*/
  date_from_str: function (d_str, spliter = ".") { try { d1 = new Date(...d_str.split(spliter).reverse()); return d1.setMonth(d1.getMonth() - 1) } catch (e) { L(e, "на elm: ", d_str) } },
  convertDateColumnToLocaleDate: function (table, col) {
    // преобразует объект Date в колонке col таблицы table в формат "18.05.2020". Возвращает таблицу с преобразованной колонкой дат.
    table = table.map(function (x) {
      x[col] = new Date(x[col]).toLocaleDateString("ru-RU")
      return x
    })
    return table
  },
  convertDateColumnToUTCDate: function (table, col) {
    // преобразует объект Date в колонке col таблицы table в формат "18.05.2020". Возвращает таблицу с преобразованной колонкой дат.
    table = table.map(function (x) {
      x[col] = new Date(x[col]).toDateString("ru-RU")
      return x
    })
    return table
  },
  convertDateColumnTo_3hDate: function (table, col) {
    // преобразует объект Date в колонке col таблицы table в формат "18.05.2020". Возвращает таблицу с преобразованной колонкой дат.
    table = table.map(function (x) {
      x[col] = Utilities.formatDate(new Date(x[col]), "GMT+3:00", "dd-MM-yyyy")
      return x
    })
    return table
  },
  findRowAfterDate: function (dateSer, rangeStrToLookFor) {
    let row
    const findSerialDate = Math.round(dateSer);
    let data = ApiUtils.readRangeSS(SSId, rangeStrToLookFor, "UNFORMATTED_VALUE", "SERIAL_NUMBER");
    if (data.length) { row = data.findLastIndex(row => row[0] < findSerialDate) };
    if (row === undefined || row === -1) { row = 0 } else { row++ };
    return row;
  },
  findRowDaysBefore: function (daysBeforeNow = 5, rangeStrToLookFor = "История затрат(api)!A2:A") {
    let row
    const findSerialDate = Math.round(this.getSerialNumberDate(new Date())) - daysBeforeNow;
    let data = ApiUtils.readRangeSS(SSId, rangeStrToLookFor, "UNFORMATTED_VALUE", "SERIAL_NUMBER");
    if (data.length) { row = data.findLastIndex(row => row[0] < findSerialDate) };
    if (row === undefined || row === -1) { row = 0 } else { row++ };
    return row;
  },
  convertDate: function (d) { return Utilities.formatDate(d, "GMT+03", "yyyy-MM-dd'T'HH:mm:ss'.000Z'"); },
  generateContinuousPeriodsFast: function (daysIntoPast, dateToSer) {
    const maxDaysPeriod = 31 - 1;
    const dayMs = 86400000; // 1 день в миллисекундах
    console.log(this.getDateFromSerialNumberDate(dateToSer))
    let today = dateToSer ? this.getDateFromSerialNumberDate(dateToSer) : Date.now();
    today = Math.floor(today / dayMs) * dayMs; // Текущая дата без времени
    const startDate = today - daysIntoPast * dayMs;

    const periods = [];
    let periodStart = startDate;
    let periodEnd = periodStart + maxDaysPeriod * dayMs;

    while (periodStart <= today) {
      periods.push({
        start: formatDateUTC(periodStart),
        end: formatDateUTC(Math.min(periodEnd, today)),
      });
      periodStart = periodEnd;
      periodEnd = periodStart + maxDaysPeriod * dayMs;
    }

    return periods;

    function formatDateUTC(timestamp) {
      return new Date(timestamp).toISOString().slice(0, 10);
    }
  },
  formatDateTo_dd_MM_yyyy: function (dateStr) { return dateStr.slice(0, 10).split("-").reverse().join(".") },
  reversDateStToSort: function (dateStr, spliter = ".") {
    //revers "01.01.2020" or "01-01-2020" to 20200101 for sorting
    try {
      return dateStr.split(spliter).reverse().join("");
    } catch (e) {
      L(e.stack, "на elm: ", dateStr);
    }
  },
};
const OtherUtils = {
  getRowIndexDateGteToday: function (data, column, day_before = 0, isDateObj = false) {
    // возвращает номер строки таблицы data, в колонке column которой дата больше или равна значение Сегодня-day_before
    // поиск идет с последней строки, если не находит дату возвращает 0
    let dates_values = data.map((row) => { return DateUtils.date_from_str(row[column]) })
    const date_gte_value = new Date(new Date(new Date().getTime() - day_before * 864e5).toDateString()).valueOf()
    const isLargeOrEqual = (element) => element < date_gte_value;
    const row_index = dates_values.findLastIndex(isLargeOrEqual)
    // L("getRowIndexDateGteToday: ", row_index)
    return row_index === -1 ? row_index : row_index + 1
  },
  _objToArray_: (a) => a.map(i => Object.values(i)),
  _objToTitArray_: (a) => a.length ? [Object.keys(a[0])].concat(_objToArray_(a)) : [],
  _ruDate_: (d) => new Date(d).toString("ru").slice(0, 10),
  _range_: (n, start, end) => [...Array(n).keys()].slice(start, end),
  _CSVtoArray_: function (data) {
    if (data.status != "success") {
      console.error(data.error);
      throw new Error('Ошибка при создании отчета!');
    }
    data = UrlFetchApp.fetch(data.file)
    data = data.getContentText()
    return Utilities.parseCsv(data, ";")
  },
  /** загружает CSV или XLSX как таблицу **/
  getDataFile: function (url) {
    // console.log( url );
    let data = UrlFetchApp.fetch(url);
    if (url.indexOf(".xlsx") > -1) {
      let file = data.getBlob();
      let config = {
        title: "[Google Sheets] " + url,
        mimeType: MimeType.GOOGLE_SHEETS
      };
      file = Drive.Files.insert(config, file);
      data = SpreadsheetApp.openById(file.getId());
      data = data.getSheets()[0].getDataRange().getValues();
      Drive.Files.remove(file.getId())
    } else {
      data = data.getContentText();
      data = Utilities.parseCsv(data, ";");
    }
    return data
  },
};
const SysUtils = {
  pagination: function (arr, perPage) {
    let start = 0;
    let pages = [];
    while (start < arr.length) {
      pages.push(arr.slice(start, start + perPage));
      start += perPage;
    }
    return pages
  },
  checkResponseCode: function (respCode) {
    if (respCode === 401) {
      throw new Error(`Ошибка авторизации, проверьте ключ авторизации.`);
    }
    if (respCode === 400) {
      throw new Error(`Ошибка в теле запроса. Code 400`);
    }
    if (respCode === 429) {
      throw new Error(`Превышено допустимое кол-во запросов в единицу времени. Code 429`);
    }
    if (respCode >= 500) {
      throw new Error(`Сервер не откликается. Повторите запрос через некоторое время. Code ${respCode}`);
    }
    if (respCode !== 200) {
      throw new Error(`Сервер вернул ошибку. Code ${respCode}`);
    }
    return true
  }
};
function getkey(keyType = "stat") {
  let keyname = '', msg = ''
  if (keyType == "rekl") {
    rangeName = "B2";
    keyname = "'РЕКЛАМА'"
  }
  else if (keyType === "stand") {
    rangeName = "B3";
    keyname = "'КОНТЕНТ'"
  }
  else if (keyType === "analytics") {
    rangeName = "B20";
    keyname = "'АГАЛИТИКА'"
  }
  else {
    rangeName = "B1";
    keyname = "'СТАТИСТИКА'"
  };
  try {
    key = SS.getSheetByName("Параметры").getRange(rangeName).getValue();
  } catch (e) {
    msg = 'Ошибка доступа к ключу: ' + e
    // SpreadsheetApp.getActiveSpreadsheet().toast(msg, 'Выполнение не возможно', -1);
    key = ""
  }
  if (key == "") {
    msg = 'Отсутствует ключ доступа: ' + keyname
    // SpreadsheetApp.getActiveSpreadsheet().toast(msg, 'Выполнение не возможно', -1);
    inform(SS, [msg, "Ошибка!"])
  }
  return key;
}
function getLastDataRow(sheet, columnLitera) {
  try {
    let lastRow = sheet.getLastRow();
    let range = 0;
    if (lastRow > 0) { range = sheet.getRange(columnLitera + lastRow) } else {
      range = sheet.getRange(columnLitera + 1);
    }
    if (range.getValue() !== "") {
      return lastRow;
    } else {
      return range.getNextDataCell(SpreadsheetApp.Direction.UP).getRow();
    }
  }
  catch (e) {
    console.log(sheet.getName(), " - Col:  ", columnLitera, " - ошибка getLastDataRow: " + e);
    return NaN
  }
}
function inform(SS, messages = ["content", "header"], toAlert = false, toConsole = true, toToast = true) {
  try {
    if (toConsole) { console.info(...messages) }
    if (toToast) { SS.toast(...messages, 15) }
    if (toAlert) { SpreadsheetApp.getUi().alert(messages[0]) }
  } catch (e) { if (e.message !== "Cannot call SpreadsheetApp.showNotification() from this context.") console.log(e.message); }
}
function removeRowsOverLimitTrigg(sheetName) {
  let limitRows, triggerFuncName;
  if (sheetName) {
    const SS = SpreadsheetApp.getActive();
    const ss = SS.getSheetByName(sheetName);
    switch (sheetName) {
      case "Аналитика (api)":
        limitRows = SS.getRange(`Параметры!B8`).getValue();
        triggerFuncName = "clearAnalyticsRowsTrigg";
        limitRows = limitRows || 100000;
        break;
      case "Заказы (api)":
        limitRows = SS.getRange(`Параметры!B9`).getValue();
        triggerFuncName = "clearOrderRowsTrigg";
        limitRows = limitRows || 130000;
        break;
      case "Продажи (api)":
        limitRows = SS.getRange(`Параметры!B10`).getValue();
        triggerFuncName = "clearSalesRowsTrigg";
        limitRows = limitRows || 130000;
        break;
    }
    ScriptApp.getProjectTriggers().forEach((trigger) => {
      if (trigger.getHandlerFunction() === triggerFuncName) ScriptApp.deleteTrigger(trigger);
    });
    try {
      let lastDataRow = ss.getDataRange().getLastRow();
      if (lastDataRow / limitRows > 50) {   // было 1.5 для отладки установлено 50
        throw new Error(
          `Удаление строк прервано. Проверить количество удаляемых строк(lastDataRow:${lastDataRow}/limitRows:${limitRows}>1.5(${lastDataRow / limitRows
          }))`
        );
      }
      let deleteRowsQuantity = lastDataRow - limitRows - 1;
      if (deleteRowsQuantity > 0) {
        ss.deleteRows(2, deleteRowsQuantity);
        console.log("Очищено строк:", deleteRowsQuantity);
        return deleteRowsQuantity;
      }
    } catch (err) {
      console.log("Ошибка при очистке строк:", err);
    }
  }
  return 0;
}
function cellsCount(SSId, log = false, bySheets = true) {
  const options = {
    includeGridData: false,
    fields:
      "sheets/properties/gridProperties/rowCount,sheets/properties/gridProperties/columnCount,sheets/properties/title",
  };
  const resp = Sheets.Spreadsheets.get(SSId, options);
  // console.log(JSON.stringify(resp));
  let obj = {}
  const cellsQuantity = resp.sheets.reduce((accum, sheet) => {
    let cellsQuantity = sheet.properties.gridProperties.columnCount * sheet.properties.gridProperties.rowCount
    obj[sheet.properties.title] = cellsQuantity
    // console.log(`${sheet.properties.title}:${cellsQuantity}`)
    return (
      accum + cellsQuantity
    );
  }, 0);
  obj = bySheets ? Object.entries(obj).sort((a, b) => b[1] - a[1]) : ""
  log &&
    console.log(
      `Количество ячеек в файле ${SpreadsheetApp.openById(SSId).getName()}: ${cellsQuantity}\n`,
      obj
    );

  return cellsQuantity;
}
function removeTriggers(handlers) {
  ScriptApp.getProjectTriggers().forEach(trigger => {
    let handler = trigger.getHandlerFunction()
    if (handlers.includes(handler)) {
      ScriptApp.deleteTrigger(trigger)
      console.log(`Script "${handler}" removed`);
    }
  })
}
function clearValues(sheetName) {
  let SSId = "1p70ytM1RbNfWtbGKTwbDAUtkTOpUw5AR4uWEU8UFQ4o"
  ranges = ApiUtils.readRangeSS(SSId, "services!A3:B")
  ranges = ranges.reduce((res, row) => { if (sheetName === row?.[0]) { res.push(row[1]) }; return res }, []);

  const SS = SpreadsheetApp.getActive(); SS.getRangeList(ranges).clearContent();
  inform(SS, [`Очищены диапазоны ${ranges}`, "Очистка диапазонов"])
}
function saveResult(resultJson, filename) {
  const folder = DriveApp.getFoldersByName("JSON").next();
  const dateTime = new Date().toLocaleString("ru")
  folder.createFile(`${dateTime}${filename}.JSON`, JSON.stringify(resultJson));
}
/**
 * Save and update array data to temp file. Delete file if (!save)
 * 
 * @param {array|object} newData - 2D-array with sheet data or object.
 * @param {string} filename - filename without extension.
 * @param {bool} save - if true - save new or update exist file.
 * @param {string} folderName - folder to save file. Folder will be create if not exists.
 * @return {array}  2D-array concatenated data.
 */
function saveAndReadDataToJSON(newData, filename, save = true, folderName = "tmp") {
  const folder = createFolderIfNotExist(folderName);;
  let filesIter = folder.getFilesByName(filename + ".JSON")
  let dataFromFile = [];

  if (filesIter.hasNext()) {
    let file = filesIter.next();// обновляем существующий
    console.log(file.getName());
    dataFromFile = JSON.parse(file.getBlob().getDataAsString());
    newData = Array.isArray(newData) ? dataFromFile.concat(newData) : Object.assign(dataFromFile, newData);
    if (save) { file.setContent(JSON.stringify(newData)) } else { file.setTrashed(true) };
  } else if (save) {
    folder.createFile(`${filename}.JSON`, JSON.stringify(newData)); // создаем новый
  }
  return newData
  function createFolderIfNotExist(folderName) {
    let folder;
    try { folder = DriveApp.getFoldersByName(folderName).next(); } catch (e) { folder = DriveApp.createFolder(folderName) };
    return folder
  }
}
function usersTriggerUpdate() {
  let SSIdService = "1p70ytM1RbNfWtbGKTwbDAUtkTOpUw5AR4uWEU8UFQ4o";
  let options = ApiUtils.readRangeSS(SSIdService, "services!G3:H");
  if (options.length) {
    const SS = SpreadsheetApp.getActive();
    const SSId = SS.getId();
    options.forEach(optionsRow => {
      let msg;
      const option = JSON.parse(optionsRow[0]);
      try {
        if (optionsRow[1] === 'setup') {
          const { handler, atHour, everyHours } = option;
          removeTriggers(handler)
          let newTrigger = ScriptApp.newTrigger(handler).timeBased();
          if (atHour !== undefined) {
            newTrigger.everyDays(1).atHour(atHour);
          } else if (everyHours) {
            newTrigger.everyHours(everyHours);
          }
          newTrigger.create();
          msg = `Обновление триггера ${handler} завершено.`;
        } else if (optionsRow[1] === 'remove') {
          const { handlers } = option;
          removeTriggers(handlers);
          msg = `Удаление триггеров ${handlers} завершено.`;
        } else if (optionsRow[1] === 'setValue') {
          const { range, value } = option;
          ApiUtils.writeToSS(SSId, value, range)
          msg = `Установка параметров в диапазон ${range} завершена.`;
        }
      } catch (e) {
        msg = `Ошибка выполнения команды ${optionsRow[1]}, ${handler || handlers}. options:${option}.`;
      }
      inform(SS, [msg, 'Users trigger update']);

    });
  } else { console.log('Не заданы параметры триггров commServiceData.'); }

}
