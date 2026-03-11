//! Zero-sized alignment marker types for UBQ block storage.

macro_rules! define_alignment {
    ($name:ident, $bytes:literal) => {
        #[doc = concat!(
                    "Zero-sized alignment marker for ",
                    stringify!($bytes),
                    "-byte block alignment."
                )]
        #[repr(align($bytes))]
        #[derive(Clone, Copy, Debug, Default)]
        pub struct $name;
    };
}

define_alignment!(A64, 64);
define_alignment!(A128, 128);
define_alignment!(A256, 256);
define_alignment!(A512, 512);
define_alignment!(A1024, 1024);
define_alignment!(A2048, 2048);
define_alignment!(A4096, 4096);
define_alignment!(A8192, 8192);
