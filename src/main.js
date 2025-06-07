function doGet(e) {
    let param = e.parameter, res;
    if (param.author) { res = "Авторизация пройдена успешно!"; }
    else { res = "unknown Apps command" }
    return ContentService.createTextOutput(res)
}

function doPost(e) {
    const params = e.parameter;
    const isAllowed = checkAllowed()
    if (!isAllowed) { return ContentService.createTextOutput('Отсутствует доступ к библиотеке. Обратитесь, пожалуйста, в техподдержку.').setMimeType(ContentService.MimeType.JSON); }
    SSId = params.ssid;
    SS = SpreadsheetApp.openById(params.ssid);
    console.log(SSId, SS.getName());
    let res = getApps(params)
    return ContentService.createTextOutput(JSON.stringify(res)).setMimeType(ContentService.MimeType.JSON)
    // return ContentService.createTextOutput(res.msg);
}

/**
 * @return txt - finish message for Content Service
 */
function getApps(params) {
    let res
    console.log(`Выполняется ${params.app} для: ${SS.getName()}`);
    switch (params.app) {
        case "getProducts":
            res = getProducts()
            break;
        case "getAdvCampList":
            res = getAdvCampList()
            break;
        case "getAdvStatistic":
            res = getAdvStatistic()
        case "statKeyWords":
            res = statKeyWords(params.propValue)
        case "singleKeyWords":
            res = singleKeyWords()
            break;
        case "analyticNmIdPeriod":
            res = analyticNmIdPeriod(params.propValue)
            break;
        case "searchTextsJam":
            res = searchTextsJam(params.propValue)
            break;
        case "getClustersToSearchText":
            res = getClustersToSearchText()
            break;
        case "testTriggSet":
            res = testTriggSet(params.propValue)
            break;
        case "restartAppTest":
            res = restartAppTest()
            break;
        default:
            console.log("Complete");
            res = 'unknown Apps command';
    }
    return res
}



function checkAllowed() {
    const allaowedSSId = '1eeld1vH9Mp_iyzx59m3m58pD5NttR-4j6cxvDANh2AE'
    const userEmail = Session.getActiveUser().getEmail();
    const allowedData = ApiUtils.readRangeSS(allaowedSSId, "Доступы!A2:B")
    const foundIdex = allowedData.findLastIndex(row => row[1] && (row[0] === userEmail));
    if (foundIdex !== -1) { return true }
    return false
}


// function doGet(e) {
//     // Узнаём, кто обратился
//     const isAllowed = checkAllowed()
//     output = ContentService.createTextOutput(JSON.stringify({ allowed: isAllowed }));
//     output.setMimeType(ContentService.MimeType.JSON);
//     return output;
// }

// function doGet_22(e) {
//     // Узнаём, кто обратился
//     const userEmail = Session.getActiveUser().getEmail();
//     // const userEmail2 = Session.getEffectiveUser().getEmail();
//     console.log(userEmail);
//     console.log("step. SSId:");
//     console.log(e.parameter.spreadsheetId);
//     // Пример: храните список в массиве (лучше - в таблице / Properties)
//     const ALLOWED_EMAILS = ["golm2020@gmail.com", "user2@gmail.com", "darifedorina@gmail.com"];

//     // Проверяем, разрешено ли
//     const isAllowed = ALLOWED_EMAILS.includes(userEmail);

//     console.log("step1. e:");
//     console.log(JSON.stringify(e));
//     console.log("step2. isAllowed:");
//     console.log(JSON.stringify(isAllowed));

//     if (!isAllowed) {
//         return ContentService
//             .createTextOutput(JSON.stringify({ error: "Access denied" }))
//             .setMimeType(ContentService.MimeType.JSON);
//     }

//     // Если доступ разрешён – выполняем логику, возвращаем результат
//     setTestvalue()

//     return ContentService
//         .createTextOutput(JSON.stringify({ message: "Hello, " + userEmail }))
//         .setMimeType(ContentService.MimeType.JSON);
// }

// // function setTestvalue(SSId) {
// //     console.log(SSId);
// //     // const name = SpreadsheetApp.getActive().getName()
// //     // SSId = "1g7TpD9YUYSN4W9ZSY8ps5M0WERRTAvFeglIEJt2Z-OY"
// //     LIB.writeToSS(SSId, [["test1:", 3.14]], "test!A5")
// // }