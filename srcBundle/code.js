/** code.gs */
// снять для отладки
// let sprs = SpreadsheetApp.openById("1ZOgynpxji02tlWHVTbgNc6soCx_7afF5bKNIjaO-9fw")
// SpreadsheetApp.setActiveSpreadsheet(sprs)
var SS = SpreadsheetApp.getActiveSpreadsheet(),
  SSId = SS.getId(),
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
  checkAccess_()
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
/**
 * временно возможный интервал получения статистики сокращен до 31 суток
 */
function getAdvStatistic() {
  checkAccess_()
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
  checkAccess_()
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
  inform(SS, [msg, SheetNames.advHistory])

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
  checkAccess_()
  //сбор информации о рекламных кампаниях на лист "Список Кампаний(api)!"
  let ss = SS.getSheetByName(SheetNames.advList);
  SheetUtils.safeRemoveFilter(ss);
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
      let newDataObj = convertAdvObj(data.adverts); // Преобразуем данные в объект {advertId:data}
      let oldData = ApiUtils.readRangeSS(SS.getId(), `${SheetNames.advList}!A2:D`, "UNFORMATTED_VALUE", "SERIAL_NUMBER");
      const oldDataLength = oldData.length;
      let oldDataObj = oldData.reduce((res, row) => {
        res[row[0]] = row;
        return res;
      }, {});
      oldDataObj = Object.assign(oldDataObj, newDataObj);
      let updatedData = Object.values(oldDataObj);
      updatedData = getAdvCampName(updatedData, options)


      updatedDataLength = updatedData.length;
      const rowsToClean = oldDataLength - updatedDataLength;
      if (rowsToClean > 0) {
        updatedData = SheetUtils.clearByAddBlankRow(updatedData, rowsToClean);
      }
      ApiUtils.writeToSS(SSId, updatedData, `${SheetNames.advList}!A2`);
      ss.getRange("E2:E").setNumberFormat("dd.MM.yyyy H:mm:ss");
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
  function getAdvCampName(updatedData, options) {
    let advertIds = updatedData.map(row => row[0]);
    let idPages = SysUtils.pagination(advertIds, 50);
    console.log(`Запросов имен РК:${idPages.length}`);
    let url = "https://advert-api.wildberries.ru/adv/v1/promotion/adverts";
    options.method = 'post';

    let objAdvIdsName = idPages.reduce((res, pageIds, id) => {
      options.payload = JSON.stringify(pageIds)
      try {
        id && Utilities.sleep(300);
        let data = UrlFetchApp.fetch(url, options);
        if (data.getResponseCode() === 200) {
          data = JSON.parse(data);
          let objNewNames = data.reduce((res, objCamp) => {
            res[objCamp.advertId] = ("'" + objCamp?.name) || "";
            return res
          }, {})
          Object.assign(res, objNewNames)
        } else {
          throw new Error('ResponseCode is not 200!')
        }
      }
      catch (e) {
        console.log(e.stack);
        `Get advName ResponseCode: ${data.getResponseCode()}. Skip page ${id}`;
      }
      return res
    }, {}
    );

    updatedData.forEach(row => {
      let advName = objAdvIdsName[row[0]];
      row.splice(1, 0, advName)
    });

    return updatedData
  }
}
function statKeyWords(SP) {
  checkAccess_()
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
    if (row[2] == 8) { campIdsSet.add(row[0]) }
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
      aggregatedObjs = saveAndReadDataToJSON_(aggregatedObjs, `temp${SSId.slice(-6)}`, (save = false));
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
        saveAndReadDataToJSON_(aggregatedObjs, `temp${SSId.slice(-6)}`, (save = true));
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
  checkAccess_()
  // propValue - номер элемента с которого продолжать загрузку
  removeTriggers(["analyticNmIdPeriodTrigg"]);
  let { dateFromSer, dateToSer } = getHeaderDateParams(SheetNames.analyticNmIdPeriod);
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
  console.log('Отобрано nmIds:', nmIds);
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
  console.log("Периоды сбора данных:", dates);
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

      let oldData = ApiUtils.readRangeSS(SSId, `${SheetNames.analyticNmIdPeriod}!A5:I`, "UNFORMATTED_VALUE", "SERIAL_NUMBER");
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
      let lastRow = SS.getSheetByName(SheetNames.analyticNmIdPeriod).getLastRow() - 4;
      let addClearRows = lastRow - table.length;
      if (addClearRows > 0) {
        table = SheetUtils.clearByAddBlankRow(table, addClearRows);
      }
      ApiUtils.writeToSS(SSId, table, `${SheetNames.analyticNmIdPeriod}!A5`);
    } else {
      msg = "Данные для обновления не были получены.";
    }
    return msg;
  }
}
function keyWords() {
  checkAccess_()
  let { dateFromSer, dateToSer } = getHeaderDateParams(SheetNames.keyWords);
  let message = "";
  const maxRequestsPerLoad = 100; // кол-во запросрв за один запуск скрипта 100
  // токен и даты с и по ---------------
  console.log(`Start "keyWords" for ${SS.getName()} spreadsheet.`);
  const headers = { Authorization: getkey("rekl") };
  // Определяем диапазон дата
  console.log(
    `Загрузка данных  с ${DateUtils.getStrDateFromSerialNumberDate(dateFromSer)} по ${DateUtils.getStrDateFromSerialNumberDate(
      dateToSer
    )}`
  );
  // Формируем сет из список Кампаний по чекбокс===true
  let campIds = ApiUtils.readRangeSS(SSId, `${SheetNames.advList}!A2:F`, "UNFORMATTED_VALUE", "SERIAL_NUMBER");
  campIds = campIds.filter((row) => row[0] && row[5]);
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
  let flowAdvId = [];
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
    console.log(`Ответ получен. Собрано строк: ${successRes.length}`);
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
    inform(SS, [message, SheetNames.keyWords]);
  }
}
function searchTextsJam(SP) {
  checkAccess_()
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
  const nmIdsTotalMax = 90, nmIdsPerReq = 30;
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
      mode: "desc",
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
      // console.log("DEBUG:", request); // DEBUG
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
function getClustersToSearchText() {
  // var url = 'http://45.155.146.48:48263/search/';
  // var url = 'http://81.177.166.230/search/';
  checkAccess_()
  var url = "https://clusters.1gb.ru/search/";
  let data = ApiUtils.readRangeSS(SSId, `${SheetNames.searchTextsJam}!B5:B`).flat(Infinity);
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
    ApiUtils.writeToSS(SSId, clusters, `${SheetNames.searchTextsJam}!O5`);
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
/**
 * check account
 */
function getAccount() {
}
// --- menu.js ---
function getMenu(box = "") {
  const ui = SpreadsheetApp.getUi();
  let menu = ui.createMenu("Меню");
  menu.addItem("1.Обновить Товары", "getProducts")
  menu.addItem("2.Обновить список РК", "getAdvCampList")
    .addSeparator()
  menu.addItem("3.Обновить историю РК", "updateHistory")
  menu.addItem("4.Обновить статистику РК", "getAdvStatistic")
    .addSeparator()
  menu.addItem("5.Обновить Аналитику Джем", "analyticNmIdPeriod")
  menu.addItem("6.Обновить keywords", "keyWords")
    .addSeparator()
  menu.addItem("7.1.Поисковый запрос Джем", "searchTextsJam")
  menu.addItem("7.2.Обновить кластеры Поисковый запрос", "getClustersToSearchText")
    .addSeparator()
    .addSeparator()
  menu.addItem("Авторизация", "JL.auth")
  menu.addToUi();
}

/** common.gs */
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
      console.log("элемент: ", t.toString(), " найден в строке #: ", old_tab_keys.indexOf(t.toString()))
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
  date_from_str: function (d_str, spliter = ".") { try { d1 = new Date(...d_str.split(spliter).reverse()); return d1.setMonth(d1.getMonth() - 1) } catch (e) { console.log(e, "на elm: ", d_str) } },
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
      console.log(e.stack, "на elm: ", dateStr);
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
function checkAccess_() {
    const clientScriptId = ScriptApp.getScriptId();
    if (!accessRights[clientScriptId]) {
      throw new Error('Access denied to this library function');
    }
    return true;
  }
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
function saveResult_(resultJson, filename) {
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
function saveAndReadDataToJSON_(newData, filename, save = true, folderName = "tmp") {
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
function include(filename) { return HtmlService.createHtmlOutputFromFile(filename).getContent(); }

/** auth */
function checkAccess_() {
  const webAppUrl = 'https://script.google.com/macros/s/AKfycbzSiS3hK8cRcfKRiCQo2GyiuxbRm3xhbPrXuwQ7NSlujM5Udr-YiC-1bedXP7Ms0ASb/exec';
  const token = ScriptApp.getOAuthToken();
  const options = {
    method: 'post',
    contentType: 'application/json',
    headers: { Authorization: 'Bearer ' + token },
    muteHttpExceptions: true
  };
  const response = UrlFetchApp.fetch(webAppUrl, options);
  const content = response.getContentText();  //console.log(content); // DEBUG console
  const code = response.getResponseCode(); // console.log("code:", code); // DEBUG console
  const needsAuth = [401, 403].includes(code) || isAuthPage_(content);
  if (needsAuth) {
    inform(SS, ['Необходимо авторизоваться: Меню->Авторизация'])
    throw new Error()
  }
  let { isAllowed, msg } = JSON.parse(content);
  if (!isAllowed) {
    inform(SS, [msg, "Внимание:"])
    throw new Error('Отсутствует доступ к библиотеке. Обратитесь в техподдержку.')
  }
  return isAllowed
}
function isAuthPage_(content) { return content.includes('<title>Sign in - Google Accounts</title>') || content.includes('<html>') }
// ## Проверка oauthScopes[]
function checkOAuthMenu_() { checkOAuth_((inf = true)); }
function checkOAuth_(inf = false) {
  const authInfo = ScriptApp.getAuthorizationInfo(ScriptApp.AuthMode.FULL);
  if (authInfo.getAuthorizationStatus() === ScriptApp.AuthorizationStatus.NOT_REQUIRED) {
    if (inf) {
      inform(SS, ["Авторизация не требуется.", "Проверка авторизации."]);
    }
    console.log(`Authorized all scopes successfully.`);
  } else {
    const scopesGranted = ScriptApp.getAuthorizationInfo(ScriptApp.AuthMode.FULL).getAuthorizedScopes();
    console.log(`Authorized scopes: ${scopesGranted} not enough.`);
    inform(SS, [`Требуется авторизация.`, "Проверка авторизации."]);
    ScriptApp.requireAllScopes(ScriptApp.AuthMode.FULL);
  }
}
function getAuthorizationHtml() {
  return HtmlService
    .createTemplateFromFile('authorization') // Файл ДОЛЖЕН быть в проекте библиотеки
    .evaluate()
    .getContent();
}
function auth() { SpreadsheetApp.getUi().showModalDialog(HtmlService.createHtmlOutput(getAuthorizationHtml()).setHeight(90), 'Авторизация'); }