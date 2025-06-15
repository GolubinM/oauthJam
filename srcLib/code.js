var SS,
  SSId,
  SheetNames = {
    products: "Товары",
    advList: "РК",
    advStat: "Cтат.Кампаний(api)",
    keyWords: "keywords",
    analyticsJam: "analiticsJam",
    searchJam: "searchJam",
    analyticNmIdPeriod: "analyticsPeriod",
    searchTextsJam: "searchText",
    advHistory: "История затрат(api)",
  };
function getProducts() {
  SS = SpreadsheetApp.openById(SSId);
  let arrProducts = [],
    formData,
    options;
  const headers = { Authorization: getkey("stand") };

  let oldData = ApiUtils.readRangeSS(SSId, `${SheetNames.products}!A1:B`);
  let lastRow = oldData?.length;
  let oldArtWB = lastRow ? oldData.slice(1).map((elm) => elm[0]) : [];
  let oldArtWBset = new Set(oldArtWB);
  getRequestOptions();
  let response,
    arrCardsObj,
    cursorCardsTotal = 100,
    arrCards = [],
    responsePage = 1;
  while (responsePage < 200) {
    response = UrlFetchApp.fetch(`https://content-api.wildberries.ru/content/v2/get/cards/list?`, options);
    SysUtils / SysUtils.checkResponseCode(response.getResponseCode());
    arrCardsObj = JSON.parse(response.getContentText());
    arrCards = arrCards.concat(arrCardsObj?.["cards"]);
    cursorCardsTotal = arrCardsObj?.["cursor"]?.["total"] || 0;
    if (cursorCardsTotal < 100) break;
    getRequestOptions(arrCardsObj?.["cursor"]);
    responsePage++;
    Utilities.sleep(400);
  }
  if (arrCards?.length) {
    arrProducts = arrCards.reduce((res, arrCard) => {
      let nmId = parseInt(arrCard["nmID"], 10);
      if (!oldArtWBset.has(nmId)) {
        res.push([nmId, arrCard["vendorCode"]]);
      }
      return res;
    }, []);
  }
  if (arrProducts?.length) {
    oldData.push(...arrProducts);
    if (lastRow == 0) {
      oldData.unshift(["vendorCode", "nmID"]);
    }
    ApiUtils.writeToSS(SSId, oldData, `${SheetNames.products}!A1`, "RAW");
  }
  let msg = `Загружено новых товаров: ${arrProducts.length}`;
  try {
    inform(SS, [msg, `${SheetNames.products}`]);
  } catch (e) { }
  //   return { msg, propValue: null, setTriggFlag: false };

  function getRequestOptions(cursorNext = "") {
    let sortObj = {
      cursor: { limit: 100 },
      filter: { withPhoto: -1 },
      sort: { ascending: true },
    };
    if (cursorNext) {
      sortObj.cursor.updatedAt = cursorNext["updatedAt"];
      sortObj.cursor.nmID = cursorNext["nmID"];
    }
    formData = {
      settings: sortObj,
    };
    options = {
      headers: headers,
      method: "post",
      contentType: "application/json",
      payload: JSON.stringify(formData),
      muteHttpExceptions: true,
    };
  }
}
function testAdv() {
  SSId = "1fVwPRevZnLnJWWMhIVNMB5j0jAidEDrueVn9khsAWwY";
  SS = SpreadsheetApp.openById(SSId);
  SpreadsheetApp.setActiveSpreadsheet(SS);
  getAdvStatistic();
}
/**
 * временно возможный интервал получения статистики сокращен до 31 суток
 */
function getAdvStatistic() {
  let msg = "";
  let { dateFromSer, dateToSer } = getHeaderDateParams(SheetNames.advStat);
  if (dateToSer - dateFromSer > 31) {
    msg = `Возможный интервал получения статистики 31 суток`;
    inform(SS, [msg, SheetNames.advStat]);
    dateFromSer = dateToSer - 31;
  }
  let dateFromStr = DateUtils.getStrDateFromSerialNumberDate(dateFromSer);
  let dateToStr = DateUtils.getStrDateFromSerialNumberDate(dateToSer);

  console.log(`Обновляем дней: ${dateToSer - dateFromSer}, с: ${dateFromStr} по: ${dateToStr}`);
  let Authorization = getkey("rekl");
  let campIds = ApiUtils.readRangeSS(SSId, `${SheetNames.advList}!A2:D`, "UNFORMATTED_VALUE", "SERIAL_NUMBER");
  // Исключаем кампании у которых status !== 9 || status !== 11 || дата изменения не позже даты запроса
  let objAdvNumberType = {}; // сбор типа кампний
  campIds = campIds.reduce((res, row) => {
    if (row[2] == 9 || row[2] == 11 || row[3] >= dateFromSer) {
      objAdvNumberType[row[0]] = row[1];
      res.push(row[0]);
    }
    return res;
  }, []);

  campIds = [...new Set(campIds)]; // чистим
  campIds.sort((a, b) => b - a); // сортируем
  console.info("всего кампаний: " + campIds.length);
  console.info("campIds: " + campIds);

  //  пагинация по long кампаний на страницу запроса
  let start = 0;
  let long = 95;
  let pages = [];
  const headers = { Authorization };
  while (start < campIds.length) {
    pages.push(campIds.slice(start, start + long));
    start += long;
  }
  // формирование запросов в массив requests
  let requests = pages.map((page) => {
    let payloads = page.map((advId) => {
      return {
        id: advId,
        interval: {
          begin: dateFromStr,
          end: dateToStr,
        },
      };
    });
    let request = {
      url: `https://advert-api.wildberries.ru/adv/v2/fullstats`,
      options: {
        method: "post",
        payload: JSON.stringify(payloads),
        headers: headers,
        muteHttpExceptions: true,
      },
    };
    return request;
  });

  console.info("requests.length is:", requests.length, " id per request:", long);

  let attempt = 0,
    maxAttempt = 3,
    gapTime = 65e3,
    table = [],
    startTime,
    waitFor = 0,
    xRatelimitRetry,
    xRatelimitRemaining = 1;
  let successRes = [],
    idReq = 0;
  for (let request of requests) {
    if (!xRatelimitRemaining && waitFor > 0) {
      Utilities.sleep(waitFor);
    }
    while (attempt < maxAttempt) {
      if (getWbResponses(request, idReq)) {
        waitFor = startTime + gapTime - Date.now();
        idReq++;
        break;
      }
      waitFor = xRatelimitRetry ? xRatelimitRetry * 1000 : gapTime;
      console.log(`Запрос с кодом!=200: ${idReq} на попытке: ${attempt} повторный запрос через: ${waitFor / 1000}`);
      attempt++;
      if (!xRatelimitRemaining && waitFor > 0) {
        Utilities.sleep(waitFor);
      }
    }
  }
  if (attempt === maxAttempt) {
    console.log(`Запрос ${idReq} не был обработан корректно.`);
  }
  // вызов функции обработки и сохранения данных
  parseAndSaveResp(successRes);
  console.log(msg);

  let triggerInterval = 1 * 6e4;
  removeTriggers(["statKeyWordsTrigg"]);
  ScriptApp.newTrigger("statKeyWordsTrigg").timeBased().after(triggerInterval).create();
  msg += "Триггер statKeyWordsTrigg установлен и сработает через 5 мин.";
  try {
    inform(SS, [msg, SheetNames.advStat]);
  } catch (e) { }

  //   return { msg, propValue: null, setTriggFlag: false };

  // запрос на WB
  function getWbResponses(request, idReq) {
    xRatelimitRetry = 0;
    let response = UrlFetchApp.fetch(request.url, request.options); // full_data - массив сформированных отправленных запросов в процессе получения данных
    startTime = Date.now();
    Utilities.sleep(5000); // время для прогрузки (подобрано для WB экспериментально)
    let respCode = response.getResponseCode();
    let headers = response.getHeaders();
    xRatelimitRemaining = +headers["x-ratelimit-remaining"] || 0;
    console.log(
      `Запрос ${idReq}: code ${respCode}, xRatelimitRemaining:${xRatelimitRemaining} ${(msg =
        respCode == 400 ? result.value?.content : "")}.`
    );
    if (msg) {
      inform(SS, [msg, "Статистика рекламы"]);
    }
    if (respCode === 429) {
      xRatelimitRetry = headers["x-ratelimit-retry"];
      console.log(respCode, `Превышено количество запросов в минуту, запрос #${idReq}, XRatelimitRetry: ${xRatelimitRetry}`);
    } else if (respCode === 200) {
      successRes.push(response.getContentText());
      return true;
    }
    return false;
  }
  // парсинг и запись
  function parseAndSaveResp(successRes) {
    table = successRes.reduce((concTab, rsp_tabData) => {
      let tabData;
      let out = [];
      tabData = JSON.parse(rsp_tabData);
      if (tabData?.length) {
        tabData.forEach(function (pload) {
          for (let day of pload?.days) {
            for (let ap of day?.apps) {
              if (ap?.appType) {
                for (let id of ap?.nm) {
                  Logger.log("nmId: %s : %s", id?.nmId, objAdvNumberType[`${id.nmId}`]);
                  out.push([
                    Math.floor(DateUtils.getSerialNumberDate(new Date(day?.date))) || "",
                    pload?.advertId || "",
                    id?.nmId || "",
                    +id?.sum || "",
                    id?.name || "",
                    id?.views || "",
                    id?.clicks || "",
                    id?.frq || "",
                    "",
                    "",
                    "",
                    id?.atbs || "",
                    id?.orders || "",
                    +id?.cr || "",
                    id?.shks || "",
                    id?.sum_price || "",
                    objAdvNumberType[`${pload?.advertId}`] || "",
                  ]);
                } // nm
              }
            } // apps
          } // days
        });
      }
      if (out.length) {
        concTab = concTab.concat(out);
      }
      return concTab;
    }, []);
    if (table.length) {
      console.log("Ответ получен. Парсим.");
      table = SheetUtils.group_list(table, [0, 1, 2, 16], [3, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);
      table = getHystorySpentForAdv(table); // расчет сумм затрат из Истории
      const rowUpdateFrom = DateUtils.findRowAfterDate(dateFromSer, `${SheetNames.advStat}!A5:A`) + 5;
      table.sort((a, b) => a[0] - b[0]);
      tableLength = table.length;
      const lastRow = SS.getSheetByName(SheetNames.advStat).getLastRow();
      const addRows = table.length - lastRow + rowUpdateFrom - 1;
      if (addRows > 0) {
        table = SheetUtils.clearByAddBlankRow(table, addRows);
      }
      let res = ApiUtils.writeToSS(SSId, table, `${SheetNames.advStat}!A${rowUpdateFrom}`);
      if (res) {
        msg += "Обновление статистики рекламных кампаний завершено. Обновлено строк: " + tableLength;
      }
    } else {
      msg += "Данные для обновления не были получены.";
    }
  }
}
function getHystorySpentForAdv(table) {
  let objStat = table.reduce((res, row) => {
    let key = `${row[0]}~${row[1]}`;
    res[key] = res[key] || { total: 0 };
    res[key].total += row[3];
    return res;
  }, {});
  let dataHistory = ApiUtils.readRangeSS(SSId, `${SheetNames.advHistory}!A5:C`, "UNFORMATTED_VALUE", "SERIAL_NUMBER");
  let objHistoryTtl = dataHistory.reduce((res, row) => {
    let key = `${row[0]}~${row[1]}`;
    res[key] = res[key] || { total: 0 };
    res[key].total += row[2];
    return res;
  }, {});
  for (let i = 0; i < table.length; i++) {
    let key = `${table[i][0]}~${table[i][1]}`;
    let percent = objStat[key] ? table[i][3] / objStat[key].total : 0;
    let sum = objHistoryTtl[key] ? percent * objHistoryTtl[key].total : 0;
    table[i].push(sum);
  }
  return table;
}
function updateHistory() {
  let { dateFromSer, dateToSer } = getHeaderDateParams(SheetNames.advHistory);
  let days = dateToSer - dateFromSer + 1;

  let sheet = SS.getSheetByName(SheetNames.advHistory);
  SheetUtils.safeRemoveFilter(sheet);
  let data = [];
  // 2.считать данные с api-WB История Кампаний
  const periods = DateUtils.generateContinuousPeriodsFast(days, dateToSer + 1);
  periods.forEach((period) => {
    let new_data = getCampHistory(period.start, period.end);
    if (new_data.length) {
      new_data = SheetUtils.group_list(new_data, [0, 1, 3, 4, 5], [2]);
      data = data.concat(new_data);
    }
  });
  data.sort((a, b) => DateUtils.reversDateStToSort(a[0]).localeCompare(DateUtils.reversDateStToSort(b[0])));
  let rowToWrite = DateUtils.findRowAfterDate(dateFromSer, `${SheetNames.advHistory}!A5:A`) + 5;
  lastRow = SS.getSheetByName(SheetNames.advHistory).getLastRow();
  let addClearRows = lastRow - (rowToWrite + data.length - 1);
  if (addClearRows > 0) {
    data = SheetUtils.clearByAddBlankRow(data, addClearRows);
  }
  ApiUtils.writeToSS(SSId, data, `${SheetNames.advHistory}!A${rowToWrite}`);
  let msg = "Обновление истории затрат завершено. Записано строк: " + data.length + " Строки обновлены со строки: " + rowToWrite;
  console.log(msg);
  return { msg, propValue: null, setTriggFlag: false };
  function getCampHistory(startDate, endDate) {
    let url = `https://advert-api.wildberries.ru/adv/v1/upd?from=${startDate}&to=${endDate}`;
    console.log(`Request data from:${startDate} to=${endDate}.`);
    let options = {
      method: "get",
      contentType: "application/json",
      muteHttpExceptions: true,
      headers: { Authorization: getkey("rekl") },
    };
    try {
      let data = UrlFetchApp.fetch(url, options);
      Utilities.sleep(1000);
      data = JSON.parse(data);
      data = data.reduce((res, d) => {
        if (d.updTime.slice(0, 10) > startDate && d.updTime.slice(0, 10) <= endDate) {
          res.push([
            d.updTime ? DateUtils.formatDateTo_dd_MM_yyyy(d.updTime) : "01.01.2100",
            d["advertId"] || "",
            d["updSum"] || "",
            d["advertType"] || "",
            d["paymentType"] || "",
            d["advertStatus"] || "",
          ]);
        }
        return res;
      }, []);
      return data;
    } catch (e) {
      console.log("ошибка загрузки данных: ", e.stack);
    }
  }
}
function getAdvCampList() {
  //сбор информации о рекламных кампаниях на лист "Список Кампаний(api)!"
  let ss = SS.getSheetByName(SheetNames.advList);
  try {
    ss.getFilter().remove();
  } catch (ex) { }
  let url = "https://advert-api.wildberries.ru/adv/v1/promotion/count";
  let options = {
    method: "get",
    contentType: "application/json",
    muteHttpExceptions: true,
    headers: { Authorization: getkey("rekl") },
  };
  let data, updatedDataLength;
  try {
    // 1. Читаем WB
    data = UrlFetchApp.fetch(url, options);
    if (data.getResponseCode() === 200) {
      // if (true) {
      data = JSON.parse(data);
      let header = ["advertId", "type", "status", "changeTime"];
      let newDataObj = convertAdvObj(data.adverts); // Преобразуем данные в объект {advertId:data}
      let oldData = ApiUtils.readRangeSS(SS.getId(), `${SheetNames.advList}!A2:D`, "UNFORMATTED_VALUE", "SERIAL_NUMBER");
      const oldDataLength = oldData.length;
      let oldDataObj = oldData.reduce((res, row) => {
        res[row[0]] = row;
        return res;
      }, {});
      oldDataObj = Object.assign(oldDataObj, newDataObj);
      let updatedData = Object.values(oldDataObj);
      updatedDataLength = updatedData.length;
      const rowsToClean = oldDataLength - updatedDataLength;
      // updatedData.sort((a, b) => a[2] - b[2])// сортировка по Date Serial Number - не сортируем чтобы не сдвигать чекбоксы отбора
      if (rowsToClean > 0) {
        updatedData = SheetUtils.clearByAddBlankRow(updatedData, rowsToClean);
      }
      updatedData.unshift(header);
      ApiUtils.writeToSS(SSId, updatedData, `${SheetNames.advList}!A1`);
      ss.getRange("D2:D").setNumberFormat("dd.MM.yyyy H:mm:ss");
    }
  } catch (e) {
    console.log(e);
  }
  let msg = "Обновление списка рекламных кампаний завершено. Записано строк: " + updatedDataLength;
  inform(SS, [msg, "Обновление списка РК"]);
  try {
    inform(SS, [msg, `${SheetNames.advList}`]);
  } catch (e) { }

  //   return { msg, propValue: null, setTriggFlag: false };

  function convertAdvObj(data) {
    const obj = data.reduce((res, adv) => {
      adv.advert_list.forEach((row) => {
        res[row.advertId] = [row.advertId, adv.type, adv.status, DateUtils.getSerialNumberDate(new Date(row.changeTime))];
      });
      return res;
    }, {});

    return obj;
  }
}
function statKeyWords(SP) {
  // период загрузки берем из статистики рекламы
  removeTriggers(["statKeyWordsTrigg"]);
  let { dateFromSer, dateToSer } = getHeaderDateParams(SheetNames.advStat);
  console.log("dateFromSer, dateToSer", dateFromSer, dateToSer);
  let message = "",
    newData;

  const maxRequestsPerLoad = 300; // кол-во запросрв за один запуск скрипта 440
  // const maxRequestsPerLoad = 50; // кол-во запросрв за один запуск скрипта 440
  const headers = { Authorization: getkey("rekl") };

  // токен и даты с и по ---------------
  console.log(`Start "statKeyWords" for ${SS.getName()} spreadsheet.`);
  // Определяем диапазон дата
  let statAB = ApiUtils.readRangeSS(SSId, `${SheetNames.advStat}!A4:B`, "UNFORMATTED_VALUE", "SERIAL_NUMBER");
  let { maxDateStatAB, minDateStatAB } = statAB.reduce(
    (res, elm) => {
      if (elm[0] > res.maxDateStatAB) res.maxDateStatAB = elm[0];
      if (elm[0] < res.minDateStatAB) res.minDateStatAB = elm[0];
      return res;
    },
    { maxDateStatAB: -Infinity, minDateStatAB: Infinity }
  );
  dateToSer = Math.min(dateToSer, maxDateStatAB);
  dateFromSer = Math.max(dateFromSer, minDateStatAB);

  console.log(
    `Загрузка данных с ${DateUtils.getStrDateFromSerialNumberDate(dateFromSer)} по ${DateUtils.getStrDateFromSerialNumberDate(
      dateToSer
    )}`
  );
  // Формируем сет из Список Кампаний(api) с условием тип кампании=8
  let campIdsSet = new Set();
  let campIds = ApiUtils.readRangeSS(SSId, `${SheetNames.advList}!A2:D`, "UNFORMATTED_VALUE", "SERIAL_NUMBER");
  campIds.forEach((row) => {
    if (row[1] == 8) {
      campIdsSet.add(row[0]);
    }
  });
  let stepDays = 6,
    finishDate = dateToSer,
    datePages = [];
  while (dateFromSer < finishDate) {
    dateToSer = Math.min(dateFromSer + stepDays, finishDate);
    datePages.push({ from: dateFromSer, to: dateToSer });
    dateFromSer = dateToSer;
  }
  let flowAdvId = [],
    flowIndRows = new Set();
  let flowRequsets = datePages.reduce((res, dataPage) => {
    //dataPage:{from:serialDate,to:serialDate}
    // Отбираем из листа Cтат.Кампаний(api) данные за период обновления
    let campList = new Set();
    statAB.forEach((row, ind) => {
      if (row[0] >= dataPage.from && row[0] <= dataPage.to && campIdsSet.has(row[1])) {
        campList.add(row[1]);
        flowIndRows.add(ind);
      }
    });
    let dateFromStr = DateUtils.getStrDateFromSerialNumberDate(dataPage.from);
    let dateToStr = DateUtils.getStrDateFromSerialNumberDate(dataPage.to);
    campList.forEach((advId) => {
      flowAdvId.push(advId);
      res.push({
        url: `https://advert-api.wildberries.ru/adv/v0/stats/keywords?advert_id=${advId}&from=${dateFromStr}&to=${dateToStr}`,
        options: {
          method: "GET",
          headers: headers,
          contentType: "application/json",
          muteHttpExceptions: true,
        },
      });
    });
    return res;
  }, []);

  // Формируем список кампаний
  let requestsLengthAll = flowRequsets.length;
  console.info("requests number is:", requestsLengthAll);
  let propValue = +(SP.getProperty("statKeyWords") || 0);
  if (propValue) {
    console.log(`Продолжаем сбор с request:${propValue * maxRequestsPerLoad}`); //продолжаем со страницы proceedFromPage
  } else {
    console.log(`Загрузка с начала списка. Максимальное кол-во запросов на сессию: ${maxRequestsPerLoad}`);
  }
  let fromInd = propValue * maxRequestsPerLoad,
    toInd = (propValue + 1) * maxRequestsPerLoad;
  flowRequsets = flowRequsets.slice(fromInd, toInd);

  let attempt = 0,
    maxAttempt = 6;
  let successRes = [],
    counterPage = 0,
    totalReq = 0;
  for (let request of flowRequsets) {
    let responseCode = null;
    counterPage++;
    if (!(counterPage % 30)) {
      console.log("requests;", counterPage);
    }
    while (attempt < maxAttempt) {
      try {
        let response = UrlFetchApp.fetch(request.url, request.options);
        Utilities.sleep(400);
        responseCode = response.getResponseCode();
        if (responseCode == 200) {
          successRes.push(response.getContentText());
          break;
        } else if (responseCode >= 429) {
          console.log(counterPage, " code:", responseCode, "waitng for 1s.");
          Utilities.sleep(1000);
        } else {
          console.log(counterPage, " code:", responseCode, "skip request.");
          attempt = maxAttempt;
        }
      } catch (e) {
        console.log("Ошибка при выполнении запроса:");
        console.log(e.message);
      }
      //сюда можно вставить промежуточную оброботку данных пока длится пауза в 60 сек.
      attempt++;
    }
    if (attempt === maxAttempt) {
      console.warn(`Ответ на запрос ${counterPage} не был получен. Код:`, responseCode);
    }
  }

  // вызов функции обработки и сохранения данных
  propValue++;
  let isFinish = false,
    msg;
  if (requestsLengthAll > propValue * maxRequestsPerLoad) {
    msg = "Устанавливаем триггер на следующий запуск. Сбор со страницы:" + propValue * maxRequestsPerLoad;
    SP.setProperty("statKeyWords", propValue);
    ScriptApp.newTrigger("statKeyWordsTrigg").timeBased().after(6e4).create();
  } else {
    msg = "Выполнение завершено. Загружены все записи.";
    SP.deleteProperty("statKeyWords");
    isFinish = true;
  }

  parseAndSaveResp(successRes, isFinish);
  try {
    inform(SS, [msg, "statKeyWords"]);
  } catch (e) { }
  console.log("campsStatKeyWords FINISHED");

  // парсинг и запись
  function parseAndSaveResp(successRes, isFinish) {
    console.log("Ответ получен. Парсинг.");
    let aggregatedObjs = successRes.reduce((allObj, rspObj, ind) => {
      //rspObj - {keywords:[{date,stats:[clicks,sum,views]},...]}
      let keywordsArr = JSON.parse(rspObj)?.keywords || [];
      let kwOutObjs = keywordsArr.reduce((res, kwordObj) => {
        //kwordObj - {date,stats:[clicks,sum,views]}
        let kwTotal = kwordObj.stats.reduce(
          (res, kw) => {
            // агрегация значений
            res.clicks += kw.clicks;
            res.sum += kw.sum;
            res.views += kw.views;
            return res;
          },
          { clicks: 0, sum: 0, views: 0 }
        );
        kwTotal.advId = flowAdvId[ind];
        kwTotal.serKwDate = DateUtils.getSerialNumberDateNoGMT(new Date(kwordObj.date));
        let key = `${kwTotal.advId}~${kwTotal.serKwDate}`;
        res[key] = kwTotal;
        return res;
      }, {}); //out = {advId~dateSer:{advId,serKwDate,clicks,sum,views},...}

      Object.assign(allObj, kwOutObjs);
      return allObj;
    }, {});

    //!!! Обновление данных с определенной строки!! с dateFrom по DateTo взять из строки
    if (isFinish) {
      aggregatedObjs = saveAndReadDataToJSON(aggregatedObjs, `temp${SSId.slice(-6)}`, (save = false));
      console.log("Всего строк данных перед записью: ", Object.keys(aggregatedObjs).length);
      if (Object.keys(aggregatedObjs).length) {
        newData = statAB.map((row, ind) => {
          if (flowIndRows.has(ind)) {
            //  если номер строки входит в набором обновляемых строк
            let key = `${row[1]}~${row[0]}`;
            if (aggregatedObjs[key]) {
              let { clicks, sum, views } = aggregatedObjs[key]; // если получен объект с таким ключом обнолвяем строку
              delete aggregatedObjs[key];
              return [clicks, sum, views];
            }
          }
          return [null, null, null]; // если не получен объект с таким ключом или строка вне диапазона обновления оставляем в файле старые значения
        });
        let res = ApiUtils.writeToSS(SSId, newData, `${SheetNames.advStat}!I5`);
        if (res) {
          message += "Все данные получены. Сбор завершен.";
        }
      } else {
        message += "Данные для обновления не были получены.";
      }
    } else {
      if (Object.keys(aggregatedObjs).length) {
        saveAndReadDataToJSON(aggregatedObjs, `temp${SSId.slice(-6)}`, (save = true));
        message += "Запись в temp-файл. Строк:" + Object.keys(aggregatedObjs).length;
      } else {
        message += "Данные для обновления не были получены.";
      }
    }
    //"Работа завершена."
    console.log(message);
    inform(SS, [message, "Обновление статистики ADV.keywords"]);
  }
}
function analyticNmIdPeriod(SP) {
  // propValue - номер элемента с которого продолжать загрузку
  removeTriggers(["analyticNmIdPeriodTrigg"]);
  let { dateFromSer, dateToSer } = getHeaderDateParams(analyticNmIdPeriod);
  const maxRequests = 14;
  // const maxRequests = 8; // DEBUG
  console.log(`Start "analyticNmIdPeriod" for ${SS.getName()} spreadsheet.`);
  const headers = { Authorization: getkey("rekl") };
  let successResults = [],
    idReq = 0;
  console.log(
    `Загрузка данных  с ${DateUtils.getStrDateFromSerialNumberDate(dateFromSer)} по ${DateUtils.getStrDateFromSerialNumberDate(
      dateToSer
    )}`
  );
  let nmIds = ApiUtils.readRangeSS(SSId, `${SheetNames.products}!A2:C`, "UNFORMATTED_VALUE", "SERIAL_NUMBER");
  nmIds = nmIds.reduce((res, row) => {
    if (row[0] && row[2]) {
      res.push(row[0]);
    }
    return res;
  }, []); // [nmId1,nmId2..]

  let payload = {
    timezone: "Europe/Moscow",
    orderBy: { field: "ordersSumRub", mode: "asc" },
    page: 1,
  };

  let stepDays = 2,
    dates = [],
    msg;
  dateToSer--;
  let nowSer = DateUtils.getSerialNumberDate(new Date());
  while (dateToSer - dateFromSer > -1) {
    let begin = DateUtils.getStrDateTimeFromSerialNumberDate(dateToSer);
    let end = DateUtils.getStrDateTimeFromSerialNumberDate(Math.min(dateToSer + 1 - 1 / 864e5, nowSer));
    dates.push({ begin, end });
    dateToSer -= stepDays;
  }
  let attempt,
    propValue,
    maxAttempt = 3,
    gapTime = 21e3,
    table = [],
    startTime,
    waitFor = 0,
    xRatelimitRetry,
    xRatelimitRemaining = 1;
  let totalRequstCount = dates.length;
  if (totalRequstCount) {
    let requests = dates.map((datesStr) => {
      payload.period = datesStr;
      return {
        url: `https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail`,
        options: {
          method: "post",
          headers,
          payload: JSON.stringify(payload),
          contentType: "application/json",
          muteHttpExceptions: true,
        },
      };
    });
    console.log(`Всего запросов:${requests.length}. Максимум запросов за 5 мин: 15.`);
    propValue = +(SP.getProperty("analyticNmIdPeriod") || 0);
    if (propValue) {
      console.log(`Сбор данных будет продолжен с запроса:${propValue * maxRequests}`);
    } else {
      console.log(`Сбор данных будет произведён с начала списка`);
    }
    requests = requests.slice(propValue * maxRequests, (propValue + 1) * maxRequests);
    console.log(`Отобрано запросов:${requests.length}. Максимум запросов за 5 мин: 15.`);

    for (let request of requests) {
      console.log(`Before request: xRatelimitRemaining && waitFor:${xRatelimitRemaining}, ${waitFor}`);
      if (!xRatelimitRemaining && waitFor > 0) {
        Utilities.sleep(waitFor);
      }
      attempt = 0;
      while (attempt < maxAttempt) {
        let result = getWbResponses(request, idReq);
        if (result === 0) {
          waitFor = startTime + gapTime - Date.now();
          idReq++;
          break;
        } else if (result === -1) {
          console.log(`Запрос с кодом!=200: ${idReq} на попытке: ${attempt} повторный запрос через: ${waitFor / 1000}`);
          attempt++;
        } else if (result > 1) {
          console.log(`Данные содержат более одной страницы. Сбор страница: ${result}`);
          request.options.payload.page = result;
          attempt += 0.5; // добавляем по 0,5 к кол-ву попыток чтобы ограничить максимальное кол-во страниц 6-ю(изменить по необходимости)
        }
        waitFor = xRatelimitRetry ? (xRatelimitRetry + 1) * 1000 : gapTime;
        if (!xRatelimitRemaining && waitFor > 0) {
          Utilities.sleep(waitFor);
        }
      }
    }
    if (attempt === maxAttempt) {
      console.log(`Запрос ${idReq} не был обработан корректно. Превышен лимит запросов.`);
    }
    console.log("successResults.length:");
    console.log(successResults.length);
    msg = parseAndSaveResp(successResults);
  } else {
    msg = `Возможно период для запроса задан не корректно.`;
  }
  propValue++;
  let finishedRequestsCount = propValue * maxRequests;
  let setTriggFlag = finishedRequestsCount < totalRequstCount ? true : false;
  
  if (setTriggFlag) {
    console.log("Устанавливаем триггер на следующий запуск скрипта через 1 минуту. Сбор со страницы:", propValue);
    SP.setProperty("analyticNmIdPeriod", propValue);
    ScriptApp.newTrigger("analyticNmIdPeriodTrigg").timeBased().after(6e4).create();
  } else {
    SP.deleteProperty("analyticNmIdPeriod");
  }
  console.log(msg);
  console.log("analyticNmIdPeriod FINISHED");

  // запрос на WB
  function getWbResponses(request, idReq) {
    console.log("Запроc: ", idReq);
    xRatelimitRetry = 0;
    let response = UrlFetchApp.fetch(request.url, request.options); // full_data - массив сформированных отправленных запросов в процессе получения данных
    startTime = Date.now();
    Utilities.sleep(500); // время для прогрузки (подобрано для WB экспериментально)
    let respCode = response.getResponseCode();
    let headers = response.getHeaders();
    xRatelimitRemaining = +(headers["x-ratelimit-remaining"] || 0);
    console.log(
      `Запрос ${idReq}: code ${respCode}, xRatelimitRemaining:${xRatelimitRemaining} ${(message =
        respCode == 400 ? response.getContentText() : "")}.`
    );
    if (message) {
      console.log(message);
    }
    if (respCode === 429) {
      xRatelimitRetry = headers["x-ratelimit-retry"];
      console.log(respCode, `Превышено количество запросов в минуту, запрос #${idReq}, XRatelimitRetry: ${xRatelimitRetry}`);
    } else if (respCode === 200) {
      let respJson = JSON.parse(response.getContentText());
      successResults.push(respJson.data);
      return respJson.hasNext ? respJson.page + 1 : 0;
    }
    return -1; // если 0 - след запрос, если -1 - повтор этого, если > 0 тот же запрос но след страница = return value
  }
  function parseAndSaveResp(successResults) {
    table = successResults.reduce((concTab, respData) => {
      let out;
      if (Array.isArray(respData.cards)) {
        out = respData.cards.reduce((res, card) => {
          res.push(
            [
              card.nmID,
              Math.floor(DateUtils.getSerialNumberDate(new Date(card.statistics.selectedPeriod.begin))),
              card.statistics.selectedPeriod.openCardCount,
              card.statistics.selectedPeriod.addToCartCount,
              card.statistics.selectedPeriod.ordersCount,
              card.statistics.selectedPeriod.ordersSumRub,
              card.statistics.selectedPeriod.buyoutsCount,
              card.statistics.selectedPeriod.buyoutsSumRub,
              card.statistics.selectedPeriod.conversions.buyoutsPercent / 100,
            ],
            [
              card.nmID,
              Math.floor(DateUtils.getSerialNumberDate(new Date(card.statistics.previousPeriod.begin))),
              card.statistics.previousPeriod.openCardCount,
              card.statistics.previousPeriod.addToCartCount,
              card.statistics.previousPeriod.ordersCount,
              card.statistics.previousPeriod.ordersSumRub,
              card.statistics.previousPeriod.buyoutsCount,
              card.statistics.previousPeriod.buyoutsSumRub,
              card.statistics.previousPeriod.conversions.buyoutsPercent / 100,
            ]
          );
          return res;
        }, []);
      }
      concTab = concTab.concat(out);
      return concTab;
    }, []);
    // SAVING
    if (table.length) {
      console.log("Ответ получен. Парсим.");
      msg = `Сбор данных analyticNmIdPeriod завершен. Добавлено строк: ${table.length}.`;

      let oldData = ApiUtils.readRangeSS(SSId, `${analyticNmIdPeriod}!A5:I`, "UNFORMATTED_VALUE", "SERIAL_NUMBER");
      let oldDataObj = oldData.reduce((res, row) => {
        let key = `${row[0]}~${row[1]}`;
        res[key] = row;
        return res;
      }, {});
      let dataObj = table.reduce((res, row) => {
        let key = `${row[0]}~${row[1]}`;
        res[key] = row;
        return res;
      }, {});
      Object.assign(oldDataObj, dataObj);
      table = Object.values(oldDataObj);

      table.sort((a, b) => a[1] - b[1]);
      let lastRow = SS.getSheetByName(analyticNmIdPeriod).getLastRow() - 4;
      let addClearRows = lastRow - table.length;
      if (addClearRows > 0) {
        table = SheetUtils.clearByAddBlankRow(table, addClearRows);
      }
      ApiUtils.writeToSS(SSId, table, `${analyticNmIdPeriod}!A5`);
    } else {
      msg = "Данные для обновления не были получены.";
    }
    return msg;
  }
}
function singleKeyWords() {
  let { dateFromSer, dateToSer } = getHeaderDateParams(SheetNames.keyWords);
  let message = "";
  const maxRequestsPerLoad = 100; // кол-во запросрв за один запуск скрипта 100
  // токен и даты с и по ---------------
  console.log(`Start "statKeyWords" for ${SS.getName()} spreadsheet.`);
  const headers = { Authorization: getkey("rekl") };
  // Определяем диапазон дата
  console.log(
    `Загрузка данных  с ${DateUtils.getStrDateFromSerialNumberDate(dateFromSer)} по ${DateUtils.getStrDateFromSerialNumberDate(
      dateToSer
    )}`
  );
  // Формируем сет из список Кампаний по чекбокс===true
  let campIds = ApiUtils.readRangeSS(SSId, `${SheetNames.advList}!A2:E`, "UNFORMATTED_VALUE", "SERIAL_NUMBER");
  campIds = campIds.filter((row) => row[0] && row[4]);
  if (campIds.length > maxRequestsPerLoad) {
    inform(SS, [
      `Кол-во отобранных кампаний > ${maxRequestsPerLoad}. Список будет ограничен последними ${maxRequestsPerLoad} кампаниями.`,
      "keyWords",
    ]);
    campIds = campIds.slice(-maxRequestsPerLoad);
  }
  let campIdsSet = new Set(); //
  campIds.forEach((row) => campIdsSet.add(row[0]));
  let stepDays = 6,
    finishDate = dateToSer,
    datePages = [];
  while (dateFromSer < finishDate) {
    dateToSer = Math.min(dateFromSer + stepDays, finishDate);
    datePages.push({ from: dateFromSer, to: dateToSer });
    dateFromSer = dateToSer;
  }
  let flowAdvId = [],
    flowIndRows = new Set();
  let flowRequsets = datePages.reduce((res, dataPage) => {
    let dateFromStr = DateUtils.getStrDateFromSerialNumberDate(dataPage.from);
    let dateToStr = DateUtils.getStrDateFromSerialNumberDate(dataPage.to);
    campIdsSet.forEach((advId) => {
      flowAdvId.push(advId);
      res.push({
        url: `https://advert-api.wildberries.ru/adv/v0/stats/keywords?advert_id=${advId}&from=${dateFromStr}&to=${dateToStr}`,
        options: {
          method: "GET",
          headers: headers,
          contentType: "application/json",
          muteHttpExceptions: true,
        },
      });
    });
    return res;
  }, []);

  // Формируем список кампаний
  let requestsLengthAll = flowRequsets.length;
  console.info("requests number is:", requestsLengthAll);
  let attempt = 0,
    maxAttempt = 6;
  let successRes = [],
    counterPage = 0;
  for (let request of flowRequsets) {
    let responseCode = null;
    counterPage++;
    if (!(counterPage % 33)) {
      console.log("requests;", counterPage);
    }

    while (attempt < maxAttempt) {
      try {
        let response = UrlFetchApp.fetch(request.url, request.options);
        Utilities.sleep(400);
        responseCode = response.getResponseCode();
        if (responseCode == 200) {
          successRes.push(response.getContentText());
          break;
        } else if (responseCode >= 429) {
          console.log(counterPage, " code:", responseCode, "waitng for 1s.");
          Utilities.sleep(1000);
        } else {
          console.log(counterPage, " code:", responseCode, "skip request.");
          attempt = maxAttempt;
        }
      } catch (e) {
        console.log("Ошибка при выполнении запроса:");
        console.log(e.message);
      }
      attempt++;
    }
    if (attempt === maxAttempt) {
      console.warn(`Ответ на запрос ${counterPage} не был получен. Код:`, responseCode);
    }
  }

  // котнтрольное сохранение результата запросов
  // let success = successRes.map(resp => JSON.parse(resp))
  // saveResult(success, "successRes")
  // return

  // вызов функции обработки и сохранения данных
  let msg = `Выполнение завершено. Загружены все записи.`;
  parseAndSaveResp(successRes);
  return { msg, propValue: null, setTriggFlag: false };

  // парсинг и запись
  function parseAndSaveResp(successRes) {
    console.log("Ответ получен.");
    let allData = successRes.reduce((totalRes, rspObj, ind) => {
      //rspObj - {keywords:[{date,stats:[clicks,sum,views]},...]}
      let keywordsArr = JSON.parse(rspObj)?.keywords || [];
      let kwOutObjs = keywordsArr.reduce((res, kwordObj) => {
        //kwordObj - {date,stats:[clicks,sum,views]}
        serKwDate = DateUtils.getSerialNumberDateNoGMT(new Date(kwordObj.date));
        kwordObj.stats = kwordObj.stats.sort((a, b) => a.views - b.views).slice(0, 100);
        let advArr = kwordObj.stats.map((row) => {
          return [serKwDate, flowAdvId[ind], row.keyword, row.clicks, row.sum, row.views];
        });
        res.push(...advArr);
        return res;
      }, []); //out = [[serKwDate,advId,keyword,clicks,sum,views],...]
      totalRes.push(...kwOutObjs);
      return totalRes;
    }, []);

    console.log("Всего строк данных перед записью: ", allData.length);
    if (allData.length) {
      allData.sort((a, b) => a[0] - b[0]);
      let header = ["date", "advId", "keywords", "clicks", "sum", "views"];
      let lastRow = SS.getSheetByName(SheetNames.keyWords).getLastRow();
      const rowsToClear = lastRow - allData.length - 2;
      if (rowsToClear > 0) {
        allData = SheetUtils.clearByAddBlankRow(allData, rowsToClear);
      }
      allData.unshift(header);
      let res = ApiUtils.writeToSS(SS.getId(), allData, `${SheetNames.keyWords}!A5`);
      if (res) {
        message += "Все данные получены. Сбор завершен.";
      }
    } else {
      message += "Данные для обновления не были получены.";
    }

    //"Работа завершена."
    console.log(message);
    inform(SS, [message, "Обновление статистики ADV.keywords"]);
  }
}
function searchTextJam() {
  analyticsJam((reportType = "SEARCH_QUERIES_PREMIUM_REPORT_TEXT"));
}
/**
 * reportTypes = ["DETAIL_HISTORY_REPORT","SEARCH_QUERIES_PREMIUM_REPORT_TEXT"]
 */
function analyticsJam(reportType = "DETAIL_HISTORY_REPORT") {
  // function analyticsJam(reportType = "DETAIL_HISTORY_REPORT") {
  // const head = ["Артикул", "Арт. продавца", "Наименование товара", "Дата", "Переходы в карточку", "Положили в корзину", "Процент выкупа"]
  // 1. Готовим запрос:
  // чтение периода
  let SS = SpreadsheetApp.getActive();
  let SSId = SS.getId();
  let headers = { Authorization: getkey("analytics") },
    sheetName;
  if (reportType === "DETAIL_HISTORY_REPORT") {
    sheetName = analyticsJam;
  } else if (reportType === "SEARCH_QUERIES_PREMIUM_REPORT_TEXT") {
    sheetName = SheetNames.searchJam;
  }
  let dataOptions = ApiUtils.readRangeSS(SSId, `${sheetName}!B1:D1`, "UNFORMATTED_VALUE", "SERIAL_NUMBER");
  let tomorrowSerial = Math.ceil(DateUtils.getSerialNumberDate(new Date()));
  let dateFromSer = dataOptions?.[0]?.[0] || tomorrowSerial - 4;
  let dateToSer = dataOptions?.[0]?.[2] || tomorrowSerial;
  let dateFrom = DateUtils.getStrDateFromSerialNumberDate(dateFromSer);
  let dateTo = DateUtils.getStrDateFromSerialNumberDate(dateToSer);
  let uuid = Utilities.getUuid(); // генерируем UUID отчета
  console.log("uuid:", uuid);
  let nmIDs = ApiUtils.readRangeSS(SSId, `${SheetNames.products}!A2:C`);
  let filteredNmIDs = nmIDs.filter((row) => row[2]);
  if (filteredNmIDs.length < 1000 && filteredNmIDs.length) {
    nmIDs = filteredNmIDs.map((row) => row[0]);
  } else {
    nmIDs = [];
  }
  let payload;
  if (reportType === "DETAIL_HISTORY_REPORT") {
    payload = {
      id: uuid,
      reportType: reportType,
      userReportName: "Card report",
      params: {
        nmIDs: nmIDs,
        startDate: dateFrom,
        endDate: dateTo,
        timezone: "Europe/Moscow",
        aggregationLevel: "day",
        skipDeletedNm: false,
      },
    };
  } else if (reportType === "SEARCH_QUERIES_PREMIUM_REPORT_TEXT") {
    payload = {
      id: uuid,
      reportType: reportType,
      userReportName: "Search report",
      params: {
        nmIDs: nmIDs,
        currentPeriod: { start: dateFrom, end: dateTo },
        topOrderBy: "orders",
        orderBy: { field: "orders", mode: "desc" },
        limit: 30,
      },
    };
  }
  let options = {
    headers: headers,
    method: "post",
    contentType: "application/json",
    payload: JSON.stringify(payload),
    muteHttpExceptions: true,
  };
  let requestReportUrl = "https://seller-analytics-api.wildberries.ru/api/v2/nm-report/downloads";
  // console.log(options);
  // return
  // Запрос на формирование отчета:
  let respCounter = 0,
    limitResp = 3;
  while (limitResp > respCounter) {
    respCounter++;
    let response = UrlFetchApp.fetch(requestReportUrl, options);
    console.log("code:", response.getResponseCode());
    console.log(response.getContentText());
    if (response.getResponseCode() === 200) {
      let respJson = JSON.parse(response.getContentText());
      let respRes = respJson?.data;
      console.log(`Статус запроса на создание отчета: ${respRes}`);
      break;
    } else {
      console.log("Код ответа на запрос:", response.getResponseCode());
      if (limitResp > respCounter) {
        console.log(`Повтор запроса через 20 секунд`);
        Utilities.sleep(20000);
      } else {
        let msg = `Лимит запросов(${limitResp}) на формирование отчета исчерпан. Попробуйте позже.`;
        inform(SS, [msg, "Jam аналитика"]);
        throw new Error(msg);
      }
    }
  }
  Utilities.sleep(20000);
  // Запрос на готовность отчета:
  respCounter = 0;
  limitResp = 20;
  while (limitResp > respCounter) {
    let isReportReadyResponse = getReportList(uuid);
    respCounter++;
    let resultArr;
    if (isReportReadyResponse.getResponseCode() == 200) {
      resultArr = JSON.parse(isReportReadyResponse.getContentText()).data;
      // console.log(resultArr);
      resultArr = resultArr.filter((row) => row.id === uuid);
    }
    if (resultArr[0].status === "SUCCESS") {
      inform(SS, [`Отчет id:${uuid} сформирован на сервере и готов к загрузке.`, "Jam аналитика"]);
      break;
    } else {
      if (limitResp > respCounter) {
        inform(SS, [`Отчет не сформирован, повтор проверки через 30 сек.`, "Jam аналитика"]);
      } else {
        let msg = `Отчет не сформирован в течении ${respCounter} попыток. Попробуйте позже`;
        inform(SS, [msg, "Jam аналитика"]);
        throw new Error(msg);
      }
    }
    Utilities.sleep(30000);
  }
  let data = getReport(uuid);
  let msg;
  if (data.length) {
    data = tabToObjectHeaders(data, reportType);
    let lastRow = SS.getSheetByName(sheetName).getLastRow();
    let rowsToClear = lastRow - data.length;
    if (rowsToClear > 0) data = SheetUtils.clearByAddBlankRow(data, rowsToClear);
    ApiUtils.writeToSS(SSId, data, `${sheetName}!A4`);
    msg = `Записано строк:${data.length}`;
  } else {
    msg = "Данные не были получены";
  }
  inform(SS, [msg, `Jam Аналитика`]);

  function getReportList(uuid) {
    let options = {
      headers: headers,
      method: "get",
      contentType: "application/json",
      muteHttpExceptions: true,
    };
    if (uuid) {
      options.filter = uuid;
    }
    let url = "https://seller-analytics-api.wildberries.ru/api/v2/nm-report/downloads";
    let response = UrlFetchApp.fetch(url, options);
    return response;
  }
  function getReport(uuid) {
    let options = {
      headers: headers,
      method: "get",
      contentType: "application/json",
      muteHttpExceptions: true,
    };
    let url = "https://seller-analytics-api.wildberries.ru/api/v2/nm-report/downloads/file/" + uuid;
    let response = UrlFetchApp.fetch(url, options);
    let zipBlob = response.getBlob();
    zipBlob.setContentType("application/zip");
    let unzipFile = Utilities.unzip(zipBlob);
    let data = Utilities.parseCsv(unzipFile[0].getDataAsString());
    return data;
  }
  function tabToObjectHeaders(table, reportType) {
    let keys = table.shift();
    let tableObjs = table.map((row) => {
      let rowObj = keys.reduce((res, key, ind) => {
        res[key] = row[ind];
        return res;
      }, {});
      return rowObj;
    });
    let fixedKeys;
    if (reportType === "DETAIL_HISTORY_REPORT") {
      fixedKeys = [
        "nmID",
        "dt",
        "openCardCount",
        "addToCartCount",
        "ordersCount",
        "ordersSumRub",
        "buyoutsCount",
        "buyoutsSumRub",
        "cancelCount",
        "cancelSumRub",
        "addToCartConversion",
        "cartToOrderConversion",
        "buyoutPercent",
      ];
    } else if (reportType === "SEARCH_QUERIES_PREMIUM_REPORT_TEXT") {
      fixedKeys = [
        "Text",
        "NmID",
        "SubjectName",
        "BrandName",
        "VendorCode",
        "Name",
        "Rating",
        "FeedbackRating",
        "MinPrice",
        "MaxPrice",
        "Frequency",
        "MedianPosition",
        "AveragePosition",
        "OpenCard",
        "OpenCardPercentile",
        "AddToCart",
        "AddToCartPercentile",
        "OpenToCart",
        "OpenToCartPercentile",
        "Orders",
        "OrdersPercentile",
        "CartToOrder",
        "CartToOrderPercentile",
        "Visibility",
      ];
    }
    let fixedTable = tableObjs.map((elm) => {
      let row = fixedKeys.map((key) => elm?.[key] || "");
      return row;
    });
    fixedTable.unshift(fixedKeys);
    return fixedTable;
  }
}
function searchTextsJam(SP) {
  removeTriggers(["searchTextsJamTrigg"]);
  let { dateFromSer, dateToSer } = getHeaderDateParams(SheetNames.searchTextsJam);
  const maxRequests = 10;
  // const maxRequests = 4; // DEBUG
  console.log(`Start "searchTextsJam" for ${SS.getName()} spreadsheet.`);
  const headers = { Authorization: getkey("rekl") };
  let successResults = [],
    idReq = 0;
  console.log(
    `Загрузка данных  с ${DateUtils.getStrDateFromSerialNumberDate(dateFromSer)} по ${DateUtils.getStrDateFromSerialNumberDate(
      dateToSer
    )}`
  );
  const nmIdsTotalMax = 90,
    nmIdsPerReq = 30;
  let nmIdsTable = ApiUtils.readRangeSS(SSId, `${SheetNames.products}!A2:C`, "UNFORMATTED_VALUE", "SERIAL_NUMBER");
  nmIdsTable = nmIdsTable.reduce((res, row) => {
    if (row[0] && row[2]) {
      res.push(row[0]);
    }
    return res;
  }, []); // [nmId1,nmId2..]
  nmIdsTable = nmIdsTable.slice(-nmIdsTotalMax);
  let nmIdsPages = SysUtils.pagination(nmIdsTable, nmIdsPerReq);
  let payload = {
    // "nmIds": nmIds,
    topOrderBy: "openToCart",
    orderBy: {
      field: "openToCart",
      mode: "asc",
    },
    limit: 30,
  };

  let dates = [],
    msg;
  while (dateToSer >= dateFromSer) {
    let currentPeriod = DateUtils.getStrDateFromSerialNumberDate(dateFromSer);
    for (let nmIds of nmIdsPages) {
      dates.push({ currentPeriod, nmIds });
    }
    dateFromSer++;
  }

  // console.log(JSON.stringify(dates, null, 2));
  // return { msg: "DEBUG searchTextsJam", propValue: null, setTriggFlag: false }

  // const SP = PropertiesService.getScriptProperties();
  let propValue = +(SP.getProperty("searchTextsJam") || 0);

  let attempt,
    maxAttempt = 3,
    gapTime = 21e3,
    table = [],
    startTime,
    waitFor = 0,
    xRatelimitRetry,
    xRatelimitRemaining = 1;
  let totalRequstCount = dates.length;
  if (totalRequstCount) {
    let requests = dates.map((datesStr) => {
      payload.nmIds = datesStr.nmIds;
      payload.currentPeriod = {
        start: datesStr.currentPeriod,
        end: datesStr.currentPeriod,
      };
      return {
        url: "https://seller-analytics-api.wildberries.ru/api/v2/search-report/product/search-texts",
        options: {
          method: "post",
          headers,
          payload: JSON.stringify(payload),
          contentType: "application/json",
          muteHttpExceptions: true,
        },
      };
    });
    console.log(`Всего запросов:${requests.length}. Максимум запросов за 5 мин: 15.`);
    propValue = +(propValue || 0);
    console.log("propValue:", propValue);
    if (propValue) {
      console.log(`Сбор данных будет продолжен с запроса:${propValue * maxRequests}`);
    }
    requests = requests.slice(propValue * maxRequests, (propValue + 1) * maxRequests);
    console.log(`Отобрано запросов:${requests.length}. Максимум запросов за 5 мин: 15.`);

    for (let request of requests) {
      console.log(`Before request: xRatelimitRemaining && waitFor:${xRatelimitRemaining}, ${waitFor}`);
      console.log("DEBUG:", request); // DEBUG
      if (!xRatelimitRemaining && waitFor > 0) {
        Utilities.sleep(waitFor);
      }
      attempt = 0;
      while (attempt < maxAttempt) {
        let result = getWbResponses(request, idReq);
        if (result === 0) {
          waitFor = startTime + gapTime - Date.now();
          idReq++;
          break;
        } else if (result === -1) {
          console.log(`Запрос с кодом!=200: ${idReq} на попытке: ${attempt} повторный запрос через: ${waitFor / 1000}`);
          attempt++;
        } else if (result > 1) {
          console.log(`Данные содержат более одной страницы. Сбор страница: ${result}`);
          request.options.payload.page = result;
          attempt += 0.5; // добавляем по 0,5 к кол-ву попыток чтобы ограничить максимальное кол-во страниц 6-ю(изменить по необходимости)
        }
        waitFor = xRatelimitRetry ? (xRatelimitRetry + 1) * 1000 : gapTime;
        if (!xRatelimitRemaining && waitFor > 0) {
          Utilities.sleep(waitFor);
        }
      }
    }
    if (attempt === maxAttempt) {
      console.log(`Запрос ${idReq} не был обработан корректно. Превышен лимит запросов.`);
    }
    console.log("successResults.length:");
    console.log(successResults.length);
    // saveResult(successResults,"successResultsJam")
    msg = parseAndSaveResp(successResults);
  } else {
    msg = `Возможно период для запроса задан не корректно.`;
  }
  propValue++;
  let finishedRequestsCount = propValue * maxRequests;
  let setTriggFlag = finishedRequestsCount < totalRequstCount ? true : false;
  
  if (setTriggFlag) {
    console.log("Устанавливаем триггер на следующий запуск скрипта через 1 минуту. Сбор со страницы:", propValue);
    SP.setProperty("searchTextsJam", propValue);
    ScriptApp.newTrigger("searchTextsJamTrigg").timeBased().after(6e4).create();
  } else {
    SP.deleteProperty("searchTextsJam");
  }

  console.log("propValue:", propValue, "setTriggFlag:", setTriggFlag);
  console.log(msg);
  console.log("searchTextsJam FINISHED");
  // return { msg, propValue, setTriggFlag };

  // запрос на WB
  function getWbResponses(request, idReq) {
    console.log("Запроc: ", idReq);
    xRatelimitRetry = 0;
    let response = UrlFetchApp.fetch(request.url, request.options); // full_data - массив сформированных отправленных запросов в процессе получения данных
    startTime = Date.now();
    Utilities.sleep(500); // время для прогрузки (подобрано для WB экспериментально)
    let respCode = response.getResponseCode();
    let headers = response.getHeaders();
    xRatelimitRemaining = +(headers["x-ratelimit-remaining"] || 0);
    console.log(
      `Запрос ${idReq}: code ${respCode}, xRatelimitRemaining:${xRatelimitRemaining} ${(message =
        respCode == 400 ? response.getContentText() : "")}.`
    );
    if (message) {
      console.log(message);
    }
    if (respCode === 429) {
      xRatelimitRetry = headers["x-ratelimit-retry"];
      console.log(respCode, `Превышено количество запросов в минуту, запрос #${idReq}, XRatelimitRetry: ${xRatelimitRetry}`);
    } else if (respCode === 200) {
      let respJson = JSON.parse(response.getContentText());

      // let dateFromRequest = JSON.parse(request.options.payload).currentPeriod.start
      // let dateFields = new Date(JSON.parse(request.options.payload).currentPeriod.start)
      // let serNumDateFields = DateUtils.getSerialNumberDate(dateFields)
      // console.log('dateFromRequest:',dateFromRequest, 'dateFields:',dateFields, 'serNumDateFields:',serNumDateFields);

      respJson.data.date = DateUtils.getSerialNumberDate(new Date(JSON.parse(request.options.payload).currentPeriod.start));
      // saveResult(respJson.data, 'respJson_data') // DEBUG
      successResults.push(respJson.data);
      return respJson.hasNext ? respJson.page + 1 : 0;
    }
    return -1; // если 0 - след запрос, если -1 - повтор этого, если > 0 тот же запрос но след страница = return value
  }

  function parseAndSaveResp(successResults) {
    table = successResults.reduce((concTab, respData) => {
      let out,
        currentDate = Math.floor(respData.date);
      if (Array.isArray(respData.items)) {
        out = respData.items.reduce((res, item) => {
          res.push([
            currentDate,
            item.text,
            item.nmId,
            item.frequency.current,
            item.weekFrequency,
            item.medianPosition.current,
            item.avgPosition.current,
            item.openCard.current,
            item.openCard.percentile,
            item.addToCart.current,
            item.addToCart.percentile,
            item.orders.current,
            item.orders.percentile,
            item.visibility.current,
          ]);
          return res;
        }, []);
      }
      concTab = concTab.concat(out);
      return concTab;
    }, []);
    // SAVING
    if (table.length) {
      console.log("Ответ получен. Парсим.");
      msg = `Сбор данных analyticNmIdPeriod завершен. Добавлено строк: ${table.length}.`;

      let oldData = ApiUtils.readRangeSS(SSId, `${SheetNames.searchTextsJam}!A5:N`, "UNFORMATTED_VALUE", "SERIAL_NUMBER");
      let oldDataObj = oldData.reduce((res, row) => {
        let key = `${row[0]}~${row[1]}~${row[2]}`;
        res[key] = row;
        return res;
      }, {});
      let dataObj = table.reduce((res, row) => {
        let key = `${row[0]}~${row[1]}~${row[2]}`;
        res[key] = row;
        return res;
      }, {});
      Object.assign(oldDataObj, dataObj);
      table = Object.values(oldDataObj);

      table
        .sort((a, b) => a[2] - b[2])
        .sort((a, b) => `${a[1]}`.localeCompare(`${b[1]}`))
        .sort((a, b) => a[0] - b[0]);
      let lastRow = SS.getSheetByName(SheetNames.searchTextsJam).getLastRow() - 4;
      let addClearRows = lastRow - table.length;
      if (addClearRows > 0) {
        table = SheetUtils.clearByAddBlankRow(table, addClearRows);
      }
      ApiUtils.writeToSS(SSId, table, `${SheetNames.searchTextsJam}!A5`);
    } else {
      msg = "Данные для обновления не были получены.";
    }
    return msg;
  }
}
function searchReportGroupJam(SP) {
  removeTriggers(["searchReportGroupJamTrigg"]);
  let { dateFromSer, dateToSer } = getHeaderDateParams(searchTextsJam);
  const maxRequests = 10;
  // const maxRequests = 4; // DEBUG
  console.log(`Start "searchTextsJam" for ${SS.getName()} spreadsheet.`);
  const headers = { Authorization: getkey("rekl") };
  let successResults = [],
    idReq = 0;
  console.log(
    `Загрузка данных  с ${DateUtils.getStrDateFromSerialNumberDate(dateFromSer)} по ${DateUtils.getStrDateFromSerialNumberDate(
      dateToSer
    )}`
  );
  const nmIdsTotalMax = 90,
    nmIdsPerReq = 30;
  let nmIdsTable = ApiUtils.readRangeSS(SSId, `${SheetNames.products}!A2:C`, "UNFORMATTED_VALUE", "SERIAL_NUMBER");
  nmIdsTable = nmIdsTable.reduce((res, row) => {
    if (row[0] && row[2]) {
      res.push(row[0]);
    }
    return res;
  }, []); // [nmId1,nmId2..]
  nmIdsTable = nmIdsTable.slice(-nmIdsTotalMax);
  let nmIdsPages = SysUtils.pagination(nmIdsTable, nmIdsPerReq);
  let payload = {
    // "nmIds": nmIds,
    topOrderBy: "openToCart",
    orderBy: {
      field: "avgPosition",
      mode: "asc",
    },
    limit: 30,
  };

  let dates = [],
    msg;
  while (dateToSer >= dateFromSer) {
    let currentPeriod = DateUtils.getStrDateFromSerialNumberDate(dateFromSer);
    for (let nmIds of nmIdsPages) {
      dates.push({ currentPeriod, nmIds });
    }
    dateFromSer++;
  }
 
  let propValue = +(SP.getProperty("searchReportGroupJam") || 0);

  let attempt,
    maxAttempt = 3,
    gapTime = 21e3,
    table = [],
    startTime,
    waitFor = 0,
    xRatelimitRetry,
    xRatelimitRemaining = 1;
  let totalRequstCount = dates.length;
  if (totalRequstCount) {
    let requests = dates.map((datesStr) => {
      payload.nmIds = datesStr.nmIds;
      payload.currentPeriod = {
        start: datesStr.currentPeriod,
        end: datesStr.currentPeriod,
      };
      return {
        url: "https://seller-analytics-api.wildberries.ru/api/v2/search-report/product/search-texts",
        options: {
          method: "post",
          headers,
          payload: JSON.stringify(payload),
          contentType: "application/json",
          muteHttpExceptions: true,
        },
      };
    });
    console.log(`Всего запросов:${requests.length}. Максимум запросов за 5 мин: 15.`);
    propValue = +(propValue || 0);
    console.log("propValue:", propValue);
    if (propValue) {
      console.log(`Сбор данных будет продолжен с запроса:${propValue * maxRequests}`);
    }
    requests = requests.slice(propValue * maxRequests, (propValue + 1) * maxRequests);
    console.log(`Отобрано запросов:${requests.length}. Максимум запросов за 5 мин: 15.`);
   
    // requests = requests.slice(0, 15) // усеньшено до 7, т.к. происходят повторные запуски приложения через 2 минуты
    // requests = requests.slice(0, 7)

    for (let request of requests) {
      console.log(`Before request: xRatelimitRemaining && waitFor:${xRatelimitRemaining}, ${waitFor}`);
      if (!xRatelimitRemaining && waitFor > 0) {
        Utilities.sleep(waitFor);
      }
      attempt = 0;
      while (attempt < maxAttempt) {
        let result = getWbResponses(request, idReq);
        if (result === 0) {
          waitFor = startTime + gapTime - Date.now();
          idReq++;
          break;
        } else if (result === -1) {
          console.log(`Запрос с кодом!=200: ${idReq} на попытке: ${attempt} повторный запрос через: ${waitFor / 1000}`);
          attempt++;
        } else if (result > 1) {
          console.log(`Данные содержат более одной страницы. Сбор страница: ${result}`);
          request.options.payload.page = result;
          attempt += 0.5; // добавляем по 0,5 к кол-ву попыток чтобы ограничить максимальное кол-во страниц 6-ю(изменить по необходимости)
        }
        waitFor = xRatelimitRetry ? (xRatelimitRetry + 1) * 1000 : gapTime;
        if (!xRatelimitRemaining && waitFor > 0) {
          Utilities.sleep(waitFor);
        }
      }
    }
    if (attempt === maxAttempt) {
      console.log(`Запрос ${idReq} не был обработан корректно. Превышен лимит запросов.`);
    }
    console.log("successResults.length:");
    console.log(successResults.length);
    // saveResult(successResults,"successResultsJam")
    msg = parseAndSaveResp(successResults);
  } else {
    msg = `Возможно период для запроса задан не корректно.`;
  }
  propValue++;
  let finisedRequestsCount = propValue * maxRequests;
  let setTriggFlag = finisedRequestsCount < totalRequstCount ? true : false;
  if (setTriggFlag) {
    console.log("Устанавливаем триггер на следующий запуск скрипта через 1 минуту. Сбор со страницы:", propValue);
    SP.setProperty("searchReportGroupJam", propValue);
    ScriptApp.newTrigger("searchReportGroupJamTrigg").timeBased().after(6e4).create();
  } else {
    SP.deleteProperty("searchReportGroupJam");
  }
  console.log(msg);
  console.log("searchReportGroupJam FINISHED");

  // запрос на WB
  function getWbResponses(request, idReq) {
    console.log("Запроc: ", idReq);
    xRatelimitRetry = 0;
    let response = UrlFetchApp.fetch(request.url, request.options); // full_data - массив сформированных отправленных запросов в процессе получения данных
    startTime = Date.now();
    Utilities.sleep(500); // время для прогрузки (подобрано для WB экспериментально)
    let respCode = response.getResponseCode();
    let headers = response.getHeaders();
    xRatelimitRemaining = +(headers["x-ratelimit-remaining"] || 0);
    console.log(
      `Запрос ${idReq}: code ${respCode}, xRatelimitRemaining:${xRatelimitRemaining} ${(message =
        respCode == 400 ? response.getContentText() : "")}.`
    );
    if (message) {
      console.log(message);
    }
    if (respCode === 429) {
      xRatelimitRetry = headers["x-ratelimit-retry"];
      console.log(respCode, `Превышено количество запросов в минуту, запрос #${idReq}, XRatelimitRetry: ${xRatelimitRetry}`);
    } else if (respCode === 200) {
      let respJson = JSON.parse(response.getContentText());
      respJson.data.date = DateUtils.getSerialNumberDate(new Date(JSON.parse(request.options.payload).currentPeriod.start));
      successResults.push(respJson.data);
      return respJson.hasNext ? respJson.page + 1 : 0;
    }
    return -1; // если 0 - след запрос, если -1 - повтор этого, если > 0 тот же запрос но след страница = return value
  }

  function parseAndSaveResp(successResults) {
    table = successResults.reduce((concTab, respData) => {
      let out,
        currentDate = Math.floor(respData.date);
      if (Array.isArray(respData.items)) {
        out = respData.items.reduce((res, item) => {
          res.push([
            currentDate,
            item.text,
            item.nmId,
            item.frequency.current,
            item.weekFrequency,
            item.medianPosition.current,
            item.avgPosition.current,
            item.openCard.current,
            item.openCard.percentile,
            item.addToCart.current,
            item.addToCart.percentile,
            item.orders.current,
            item.orders.percentile,
            item.visibility.current,
          ]);
          return res;
        }, []);
      }
      concTab = concTab.concat(out);
      return concTab;
    }, []);
    // SAVING
    if (table.length) {
      console.log("Ответ получен. Парсим.");
      msg = `Сбор данных analyticNmIdPeriod завершен. Добавлено строк: ${table.length}.`;

      let oldData = ApiUtils.readRangeSS(SSId, `${SheetNames.searchTextsJam}!A5:N`, "UNFORMATTED_VALUE", "SERIAL_NUMBER");
      let oldDataObj = oldData.reduce((res, row) => {
        let key = `${row[0]}~${row[1]}~${row[2]}`;
        res[key] = row;
        return res;
      }, {});
      let dataObj = table.reduce((res, row) => {
        let key = `${row[0]}~${row[1]}~${row[2]}`;
        res[key] = row;
        return res;
      }, {});
      Object.assign(oldDataObj, dataObj);
      table = Object.values(oldDataObj);

      table
        .sort((a, b) => a[2] - b[2])
        .sort((a, b) => `${a[1]}`.localeCompare(`${b[1]}`))
        .sort((a, b) => a[0] - b[0]);
      let lastRow = SS.getSheetByName(SheetNames.searchTextsJam).getLastRow() - 4;
      let addClearRows = lastRow - table.length;
      if (addClearRows > 0) {
        table = SheetUtils.clearByAddBlankRow(table, addClearRows);
      }
      ApiUtils.writeToSS(SSId, table, `${SheetNames.searchTextsJam}!A5`);
    } else {
      msg = "Данные для обновления не были получены.";
    }
    return msg;
  }
}
function getClustersToSearchText() {
  // var url = 'http://45.155.146.48:48263/search/';
  // var url = 'http://81.177.166.230/search/';
  var url = "https://clusters.1gb.ru/search/";
  let data = ApiUtils.readRangeSS(SSId, `${searchTextsJam}!B5:B`).flat(Infinity);
  let searchValues = [...new Set(data)];
  let payload = JSON.stringify({ values: searchValues });
  let options = {
    method: "post",
    contentType: "application/json",
    payload: payload,
    // если нужен авторизационный токен, добавить headers с 'Authorization'
  };
  let response = UrlFetchApp.fetch(url, options);
  let respCode = response.getResponseCode();
  let respText = response.getContentText();
  let msg;
  if (respCode == 200) {
    let jsonResp = JSON.parse(respText);
    let objPhrase = jsonResp.results.reduce((res, row) => {
      res[row.phrase] = row.cluster;
      return res;
    }, {});
    // console.log(objPhrase);
    msg = "Получено записей " + Object.values(jsonResp.results).length;
    let clusters = data.map((phrase) => [objPhrase[phrase] || ""]);
    msg += ". Добавлено строк:" + clusters.length;
    ApiUtils.writeToSS(SSId, clusters, `${searchTextsJam}!O5`);
  } else {
    msg = `Код ответа: ${respCode}. ${respText}`;
  }
  console.log(msg);
  return { msg, propValue: null, setTriggFlag: false };
}
function getHeaderDateParams(rangeStr) {
  let dataOptions = ApiUtils.readRangeSS(SSId, `${rangeStr}!B1:B2`, "UNFORMATTED_VALUE", "SERIAL_NUMBER");
  let tomorrowSerial = Math.ceil(DateUtils.getSerialNumberDate(new Date()));
  let dateFromSer = dataOptions?.[0]?.[0] || tomorrowSerial - 4;
  let dateToSer = dataOptions?.[1]?.[0] || tomorrowSerial;
  return { dateFromSer, dateToSer };
}
// DEBUG
function testAn() {
  SSId = "1fVwPRevZnLnJWWMhIVNMB5j0jAidEDrueVn9khsAWwY";
  // SSId = "1GqUvXk1rAfQZ4O79HY6jAXgS1Q3bsV1ytXdFwIgFOzI";
  SS = SpreadsheetApp.openById(SSId);
  SpreadsheetApp.setActiveSpreadsheet(SS);
  // analyticNmIdPeriod(0)
  searchTextsJam(0);
}
function testHistory() {
  SSId = "1fVwPRevZnLnJWWMhIVNMB5j0jAidEDrueVn9khsAWwY";
  SS = SpreadsheetApp.openById(SSId);
  SpreadsheetApp.setActiveSpreadsheet(SS);
  updateHistory();
}
