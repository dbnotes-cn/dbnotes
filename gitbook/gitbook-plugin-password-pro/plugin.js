require(["gitbook", "jQuery"], function (gitbook, $) {
    // book.js 参数信息
    var inputs;

    // 全局参数
    var globalPassword;
    var globalTip;
    var globalErrorTip;
    var globalReject;

    // 使用日期进行混淆加密
    const S_KEY = new Date();

    // 已经验证的参数列表
    var okPage = {};

    gitbook.events.bind('start', function (e, config) {
        inputs = config['password-pro'];

        // 匹配 global 信息
        globalTip = inputs.tip;
        globalErrorTip = inputs.errorTip;
        globalReject = inputs.reject;

        if (inputs.password != null && inputs.password.length != 0) {
            // 设置了 password 时
            globalPassword = md5(inputs.password + S_KEY);
        } else {
            globalPassword = null;
        }
    });

    gitbook.events.bind('page.change', function (e, config) {
        // 当前页面路径:
        var id = gitbook.state.file.path;

        if (okPage[id] == null) {
            var tip = globalTip;
            var errorTip = globalErrorTip;
            var reject = globalReject;
            var password = globalPassword;

            // 查看当前页面状态
            if (inputs[id] != null) {
                if (typeof inputs[id] === "string") {
                    if (inputs[id].length == 0) {
                        // 类型为字符串, 但长度为 0, 此时识别为对该页面无密码
                        password = null;
                    } else {
                        password = md5(inputs[id] + S_KEY);
                    }
                } else {
                    // object 类型
                    if (inputs[id]["tip"] != null) {
                        tip = inputs[id]["tip"];
                    }

                    if (inputs[id]["errorTip"] != null) {
                        errorTip = inputs[id]["errorTip"];
                    }

                    if (inputs[id]["reject"] != null) {
                        reject = inputs[id]["reject"];
                    }

                    if (inputs[id]["password"] != null) {
                        if (inputs[id]["password"].length == 0) {
                            // 类型为字符串, 但长度为 0, 此时识别为对该页面无密码
                            password = null;
                        } else {
                            password = md5(inputs[id]["password"] + S_KEY);
                        }
                    }
                }
            }

            if (password != null) {
                // 如果该页面有密码
                var inputPassword = prompt(tip);
                if (inputPassword != null) {
                    // 如果用户输入了密码
                    inputPassword = md5(inputPassword + S_KEY);
                    while (inputPassword != password) {
                        // 请重新输入密码
                        inputPassword = prompt(errorTip);
                        if (inputPassword != null) {
                            // 如果用户输入了密码
                            inputPassword = md5(inputPassword + S_KEY);
                        } else {
                            // 用户点击了取消
                            alert(reject);
                            return;
                        }
                    }

                    // 密码输入正确
                    okPage[id] = true;
                } else {
                    // 用户点击了取消
                    alert(reject);
                    return;
                }
            }
        }

        // 没有密码或密码正确, 该页面设置为显示
        document.getElementsByClassName("book-body")[0].style.display = "block";
    });
});
