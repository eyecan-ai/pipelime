
import cv2
import albumentations as A


class PadIfNeededV2(A.PadIfNeeded):

    def __init__(
        self,
        min_height=1024,
        min_width=1024,
        border_mode=cv2.BORDER_REFLECT_101,
        value=None,
        mask_value=None,
        row_align="center",
        col_align="center",
        always_apply=False,
        p=1.0,
    ):
        super(PadIfNeededV2, self).__init__(
            min_height,
            min_width,
            border_mode=border_mode,
            value=value,
            mask_value=mask_value,
            always_apply=always_apply,
            p=p
        )
        self.row_align = row_align
        self.col_align = col_align

    def update_params(self, params, **kwargs):
        params = super(PadIfNeededV2, self).update_params(params, **kwargs)
        rows = params["rows"]
        cols = params["cols"]

        if rows < self.min_height:
            if self.row_align == "center":
                h_pad_top = int((self.min_height - rows) / 2.0)
                h_pad_bottom = self.min_height - rows - h_pad_top
            elif self.row_align == "top":
                h_pad_top = 0
                h_pad_bottom = self.min_height - rows
            elif self.row_align == "bottom":
                h_pad_top = self.min_height - rows
                h_pad_bottom = 0
            else:
                raise ValueError(f'Invalid value \"{self.row_align}\" for row_align')
        else:
            h_pad_top = 0
            h_pad_bottom = 0

        if cols < self.min_width:
            if self.col_align == "center":
                w_pad_left = int((self.min_width - cols) / 2.0)
                w_pad_right = self.min_width - cols - w_pad_left
            elif self.col_align == "left":
                w_pad_left = 0
                w_pad_right = self.min_width - cols
            elif self.col_align == "right":
                w_pad_left = self.min_width - cols
                w_pad_right = 0
            else:
                raise ValueError(f"Invalid value \"{self.col_align}\" for col_align")
        else:
            w_pad_left = 0
            w_pad_right = 0

        params.update(
            {"pad_top": h_pad_top, "pad_bottom": h_pad_bottom, "pad_left": w_pad_left, "pad_right": w_pad_right}
        )
        return params

    def get_transform_init_args_names(self):
        return ("min_height", "min_width", "border_mode", "value", "mask_value", "row_align", "col_align")
