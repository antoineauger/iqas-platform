$(function () {

    var timeoutiQASstatus;

    (function worker() {
        $.ajax({
            dataType: "json",
            url: '/requests',
            success: function (data) {
                $("#iqas_status_icon_nok").css("display", "none");
                $("#text_status_iqas").attr("title", "iQAS platform is currently running");
            },
            error: function (jqXHR, textStatus, errorThrown) {
                $("#iqas_status_icon_nok").css("display", "block");
                $("#text_status_iqas").attr("title", "Error: make sure that the iQAS platform is running");
            },
            complete: function (jqXHR, textStatus) {
                timeoutiQASstatus = setTimeout(worker, 5000);
            }
        });

    })();

});