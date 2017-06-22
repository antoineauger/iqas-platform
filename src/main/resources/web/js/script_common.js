$(function () {

    var timeoutiQASstatus;
    var image_iqas_nok_status = new Image();
    var image_iqas_ok_status = new Image();

    (function worker() {
        $.ajax({
            dataType: "json",
            url: '/requests',
            success: function (data) {
                image_iqas_ok_status.src = '/figures/iqas_status_ok.png';
                image_iqas_nok_status.src = '/figures/iqas_status_nok.png';

                $("#iqas_status_icon").attr("src", "/figures/iqas_status_ok.png");
                $("#iqas_status_icon").attr("title", "iQAS platform is currently running")
            },
            error: function (jqXHR, textStatus, errorThrown) {
                $("#iqas_status_icon").attr("src", "/figures/iqas_status_nok.png");
                $("#iqas_status_icon").attr("title", "Error: make sure that the iQAS platform is running")
            },
            complete: function (jqXHR, textStatus) {
                timeoutiQASstatus = setTimeout(worker, 5000);
            }
        });

    })();

});